#!/usr/bin/env python3
"""Signal detectors for procurement intelligence tenets.

Implements:
  T4  · Amendment inflation (contract amended up; next contract uses inflated base)
  T5  · Emergency procurement overuse
  T6  · Temporal clustering (year-end burst, weekend/holiday awards)
  T8  · Single-source monopoly
  T9A · Committee capture (single body dominates direct awards)
  T9B · Copy-paste structured award (same non-round amount, same supplier)
  T9C · 30-day award burst (same supplier, multiple awards in a month)
  T9D · Price standardisation across suppliers (fixed-rate manipulation)

Usage:
    python scripts/detect_signals.py --org 6166
    python scripts/detect_signals.py --org 6166 --json
    python scripts/detect_signals.py --org 6166 --tenets T5,T9B,T9C

Rules of the game
-----------------
T5  Emergency if subject contains negotiated-without-publication keywords.
    Fires at >15% annual rate OR single award >€60k.

T6a Year-end burst: Dec 22–31 daily rate vs baseline. Fires at 3×.
T6b Weekend awards ≥ €5k.
T6c Greek public holiday awards ≥ €5k.
T6d Monthly spike ≥ 2.5× annual mean.

T8  Single supplier >70% of annual direct-award spend (critical) or
    >50% for 2+ consecutive years (sustained monopoly).

T9A Committee capture. One named decision-making body (identified from
    subject text) responsible for >T9A_THRESHOLD (40%) of all direct
    awards. Structural control point: if the committee is captured, all
    maintenance spend is captured.

T9B Copy-paste structured award. Same non-round amount (not divisible
    by T9B_ROUND_DIVISOR=100) paid to the same supplier T9B_MIN_COUNT
    (2+) times. Non-round identical amounts indicate a template decision
    used repeatedly rather than independently evaluated procurements.

T9C 30-day award burst. Same supplier receives T9C_MIN_AWARDS (3+)
    separate awards within any rolling T9C_WINDOW_DAYS (30) day window.
    A genuine single need split across multiple small direct awards.

T9D Price standardisation. Same non-round amount paid to T9D_MIN_SUPPLIERS
    (3+) different suppliers. Suggests either a fixed rate applied
    mechanically (copy-paste decisions) or coordination between awards
    that should be independent.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

# ── Rule parameters ──────────────────────────────────────────────────────────

# Use accented stems — pandas case=False does NOT strip Greek accent marks,
# so "διαπραγματ" will NOT match "διαπραγμάτευσης". Use the accented stem.
T5_KEYWORDS = (
    "διαπραγμάτευσ",       # covers all inflections: -η, -ης, -ησης, -ήσει
    "αδυναμία ανταγωνισμ",
    "αδυναμια ανταγωνισμ",
    "μοναδικός προμηθευτ",
    "μοναδικος προμηθευτ",
)
# Subjects containing these phrases are council procedural decisions
# (urgent agenda items), NOT emergency procurement — exclude them
T5_EXCLUSION_KEYWORDS = (
    "χαρακτηρισμός θέματος",
    "χαρακτηρισμος θεματος",
    "ημερήσια διάταξη",
    "ημερησια διαταξη",
)
T5_THRESHOLD = 0.15       # 15% of procurement decisions
T5_HIGH_VALUE = 60_000    # €60k — double the direct-award ceiling

T6_YEAREND_START_DAY = 22  # Dec 22–31
T6_YEAREND_MULTIPLIER = 3.0
T6_MIN_YEAR_DECISIONS = 20   # min procurement decisions in a year for T6a to fire
                              # (was 3000 all-decisions; now scoped to procurement types)
T6_WEEKEND_AMOUNT = 5_000  # €5k minimum for a weekend award to be flagged
T6_MONTHLY_MULTIPLIER = 2.5

T4_AMOUNT_TOLERANCE = 0.05   # consecutive awards within 5% considered price carry-forward
T4_MIN_AMOUNT = 50_000        # ignore carry-forwards below €50k
T4_MAX_GAP_DAYS = 548         # max gap between consecutive contracts (~18 months)
T4_AMENDMENT_KEYWORDS = (
    "τροποποίηση",
    "τροποποιητικ",
    "τροποπ.",
    "τροπ. συμβ",
    "αύξηση αντικειμένου",
    "αυξηση αντικειμενου",
)

T1_COUNT_THRESHOLD = 0.30   # supplier with >30% of org's 3-year direct-award count
T1_AMOUNT_THRESHOLD = 0.25  # or >25% of 3-year direct-award spend
T1_MIN_AWARDS = 2           # minimum awards to be considered

T8_SHARE_THRESHOLD = 0.50  # 50% of annual direct-award spend
T8_SHARE_CRITICAL = 0.70   # 70% — fire regardless of persistence
T8_MIN_YEARS = 2           # consecutive years above threshold

T9A_THRESHOLD = 0.40       # 40% of direct awards from one committee/body
T9B_ROUND_DIVISOR = 100    # amounts divisible by this are considered "round"
T9B_MIN_COUNT = 2          # same non-round amount to same supplier
T9C_MIN_AWARDS = 3         # minimum awards in burst window
T9C_WINDOW_DAYS = 30       # rolling window in days
T9D_MIN_SUPPLIERS = 3      # same non-round amount across this many suppliers

PROCUREMENT_TYPES = {
    "ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ",
    "ΣΥΜΒΑΣΗ",
    "ΚΑΤΑΚΥΡΩΣΗ",
    "Procurement assignment",
    "Contract award",
}

# Subset of PROCUREMENT_TYPES that represents genuine direct awards (no tender).
# ΚΑΤΑΚΥΡΩΣΗ (competitive tender finalisation) and ΣΥΜΒΑΣΗ (signed contract, may
# follow a tender) are excluded — they represent competitive procedures and should
# NOT be flagged as single-source monopolies.
DIRECT_AWARD_TYPES = {
    "ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ",
    "Procurement assignment",
}

# Greek public holidays (recurring dates, MM-DD)
GREEK_HOLIDAYS = {
    "01-01", "01-06", "03-25", "05-01", "08-15",
    "10-28", "12-25", "12-26",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_decisions(org: str) -> pd.DataFrame:
    path = REPO_ROOT / "data" / "normalized" / f"org={org}" / "decisions.csv"
    if not path.exists():
        sys.exit(f"No decisions.csv found at {path}")
    df = pd.read_csv(path, low_memory=False)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # issue_date may be ISO string ("2022-01-15") or millisecond timestamp
    # ("1546387200000") depending on how the decision was hydrated.
    # Normalise both to datetime.
    def parse_dates(col: pd.Series) -> pd.Series:
        # Try standard parsing first
        parsed = pd.to_datetime(col, errors="coerce", infer_datetime_format=True)
        # For rows that failed, try treating as ms epoch
        ms_mask = parsed.isna() & col.notna()
        if ms_mask.any():
            numeric = pd.to_numeric(col[ms_mask], errors="coerce")
            parsed[ms_mask] = pd.to_datetime(numeric, unit="ms", errors="coerce")
        return parsed

    df["issue_date"] = parse_dates(df["issue_date"])
    df["year"] = df["issue_date"].dt.year
    df["month"] = df["issue_date"].dt.month
    df["day"] = df["issue_date"].dt.day
    df["weekday"] = df["issue_date"].dt.dayofweek  # 0=Mon … 6=Sun
    df["mmdd"] = df["issue_date"].dt.strftime("%m-%d")
    return df


def is_procurement(df: pd.DataFrame) -> pd.Series:
    return df["decision_type"].isin(PROCUREMENT_TYPES)


def subject_contains(df: pd.DataFrame, keywords: tuple) -> pd.Series:
    pat = "|".join(keywords)
    return df["subject"].str.contains(pat, case=False, na=False)


def fmt_eur(v: float) -> str:
    return f"€{v:,.0f}"


def firing(label: str, details: list[dict]) -> dict:
    return {"tenet": label, "fired": True, "findings": details}


def clean(details: list[dict]) -> list[dict]:
    return [clean_row(d) for d in details]


def clean_row(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if isinstance(v, float) and v != v:
            out[k] = None
        elif hasattr(v, "item"):
            out[k] = v.item()
        else:
            out[k] = v
    return out


# ── T4 · Amendment inflation ─────────────────────────────────────────────────

def detect_t4(df: pd.DataFrame) -> dict:
    """T4: Amendment inflation — contract amended upward; next contract starts at
    the inflated value rather than the original.

    Pattern (confirmed at ΓΝ ΤΡΙΚΑΛΑ and ΓΝ ΛΑΜΙΑ):
      1. ΑΝΑΘΕΣΗ(N): supplier receives direct award at €X
      2. ΣΥΜΒΑΣΗ amendment raises it to €X+δ
      3. ΑΝΑΘΕΣΗ(N+1): same supplier, same service, starts at €X+δ (not €X)

    Detection: for each supplier with 2+ direct awards, flag consecutive pairs
    where amount[N+1] is within T4_AMOUNT_TOLERANCE of amount[N] and both
    exceed T4_MIN_AMOUNT.  If an intervening ΣΥΜΒΑΣΗ amendment is found
    the rule fires as "amendment_inflation"; otherwise as "price_carry".
    """
    awards = df[
        df["decision_type"].isin(DIRECT_AWARD_TYPES) &
        df["amount"].notna() &
        (df["amount"] >= T4_MIN_AMOUNT) &
        df["supplier_tax_id"].notna() &
        df["issue_date"].notna()
    ].copy()
    awards = awards.sort_values(["supplier_tax_id", "issue_date"])

    # Find ΣΥΜΒΑΣΗ records that look like amendments
    symbasi = df[df["decision_type"] == "ΣΥΜΒΑΣΗ"].copy()
    if not symbasi.empty and "subject" in symbasi.columns:
        amend_mask = subject_contains(symbasi, T4_AMENDMENT_KEYWORDS)
        amendments = symbasi[amend_mask & symbasi["supplier_tax_id"].notna() & symbasi["issue_date"].notna()]
    else:
        amendments = pd.DataFrame()

    findings = []

    for supplier_id, grp in awards.groupby("supplier_tax_id"):
        grp = grp.sort_values("issue_date").reset_index(drop=True)
        if len(grp) < 2:
            continue

        sup_amendments = (
            amendments[amendments["supplier_tax_id"] == supplier_id]
            if not amendments.empty else pd.DataFrame()
        )

        for i in range(len(grp) - 1):
            row_n  = grp.iloc[i]
            row_n1 = grp.iloc[i + 1]

            amt_n  = float(row_n["amount"])
            amt_n1 = float(row_n1["amount"])
            if amt_n <= 0 or amt_n1 <= 0:
                continue

            gap_days = (row_n1["issue_date"] - row_n["issue_date"]).days
            if gap_days < 1 or gap_days > T4_MAX_GAP_DAYS:
                continue

            pct_diff = abs(amt_n1 - amt_n) / amt_n
            if pct_diff > T4_AMOUNT_TOLERANCE:
                continue

            # Look for an intervening amendment between the two contracts
            has_amendment = False
            amendment_ada = None
            amendment_amount = None
            if not sup_amendments.empty:
                between = sup_amendments[
                    (sup_amendments["issue_date"] >= row_n["issue_date"]) &
                    (sup_amendments["issue_date"] <= row_n1["issue_date"])
                ]
                if not between.empty:
                    has_amendment = True
                    with_amt = between[between["amount"].notna()]
                    if not with_amt.empty:
                        best = with_amt.loc[with_amt["amount"].idxmax()]
                        amendment_ada = str(best.get("ada", "")) or None
                        amendment_amount = float(best["amount"])

            findings.append({
                "rule": "amendment_inflation" if has_amendment else "price_carry",
                "supplier_tax_id": str(supplier_id),
                "supplier_name": (
                    str(row_n["supplier_name"])
                    if pd.notna(row_n.get("supplier_name")) else None
                ),
                "contract_n": {
                    "ada": str(row_n.get("ada", "")) or None,
                    "date": str(row_n["issue_date"])[:10],
                    "amount": amt_n,
                    "subject": str(row_n.get("subject", ""))[:80],
                },
                "contract_n1": {
                    "ada": str(row_n1.get("ada", "")) or None,
                    "date": str(row_n1["issue_date"])[:10],
                    "amount": amt_n1,
                    "subject": str(row_n1.get("subject", ""))[:80],
                },
                "gap_days": int(gap_days),
                "pct_diff": round(pct_diff * 100, 2),
                "has_amendment": has_amendment,
                "amendment_ada": amendment_ada,
                "amendment_amount": amendment_amount,
            })

    # One finding per supplier — prefer amendment_inflation over price_carry,
    # then largest amount
    seen: set = set()
    deduped = []
    findings.sort(key=lambda x: (0 if x["rule"] == "amendment_inflation" else 1,
                                  -x["contract_n"]["amount"]))
    for f in findings:
        if f["supplier_tax_id"] not in seen:
            seen.add(f["supplier_tax_id"])
            deduped.append(f)

    return {
        "tenet": "T4",
        "name": "Amendment inflation",
        "fired": len(deduped) > 0,
        "findings": clean(deduped),
    }


# ── T5 · Emergency procurement overuse ───────────────────────────────────────

def detect_t5(df: pd.DataFrame) -> dict:
    procurement = df[is_procurement(df)].copy()
    emergency = procurement[
        subject_contains(procurement, T5_KEYWORDS) &
        ~subject_contains(procurement, T5_EXCLUSION_KEYWORDS)
    ].copy()

    findings = []

    # Rule 1: annual rate
    for year, grp in procurement.groupby("year"):
        total = len(grp)
        em_grp = emergency[emergency["year"] == year]
        em_count = len(em_grp)
        rate = em_count / total if total else 0
        if rate >= T5_THRESHOLD:
            findings.append({
                "rule": "annual_rate",
                "year": int(year),
                "emergency_decisions": int(em_count),
                "total_procurement_decisions": int(total),
                "rate": round(rate, 3),
                "threshold": T5_THRESHOLD,
                "sample_subjects": em_grp["subject"].dropna().head(3).tolist(),
            })

    # Rule 2: high-value emergency awards
    high = emergency[emergency["amount"].notna() & (emergency["amount"] > T5_HIGH_VALUE)]
    for _, row in high.iterrows():
        findings.append({
            "rule": "high_value_emergency",
            "ada": row.get("ada"),
            "year": int(row["year"]) if pd.notna(row.get("year")) else None,
            "amount": float(row["amount"]),
            "subject": str(row.get("subject", ""))[:120],
            "threshold": T5_HIGH_VALUE,
        })

    return {
        "tenet": "T5",
        "name": "Emergency procurement overuse",
        "total_emergency_decisions": len(emergency),
        "total_procurement_decisions": len(procurement),
        "overall_rate": round(len(emergency) / len(procurement), 3) if len(procurement) else 0,
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── T6 · Temporal clustering ─────────────────────────────────────────────────

def detect_t6(df: pd.DataFrame) -> dict:
    # Scope T6 to procurement decision types only.
    # ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ (budget commitments) and ΑΝΑΤΡΟΠΗ (reversals) are posted
    # in large batches at year-end and on public holidays as routine accounting
    # operations — NOT procurement anomalies.  Including them produces false positives
    # for every hospital and municipality that batches financial entries at Dec 31.
    proc = df[is_procurement(df)].copy()
    findings = []

    # ── T6a: year-end burst ──
    for year, grp in proc.groupby("year"):
        grp = grp[grp["issue_date"].notna()].copy()
        if len(grp) < T6_MIN_YEAR_DECISIONS:
            continue  # sparse year — data incomplete, baseline unreliable

        yearend = grp[(grp["month"] == 12) & (grp["day"] >= T6_YEAREND_START_DAY)]
        baseline = grp[~((grp["month"] == 12) & (grp["day"] >= T6_YEAREND_START_DAY))]

        ye_days = 31 - T6_YEAREND_START_DAY + 1  # 10 days
        bl_days = 355  # approximate remaining days

        ye_rate = len(yearend) / ye_days
        bl_rate = len(baseline) / bl_days if bl_days else 0

        multiplier = ye_rate / bl_rate if bl_rate else 0

        if multiplier >= T6_YEAREND_MULTIPLIER:
            findings.append({
                "rule": "yearend_burst",
                "year": int(year),
                "yearend_decisions": int(len(yearend)),
                "yearend_daily_rate": round(ye_rate, 1),
                "baseline_daily_rate": round(bl_rate, 1),
                "multiplier": round(multiplier, 1),
                "threshold_multiplier": T6_YEAREND_MULTIPLIER,
            })

    # ── T6b: weekend high-value awards ──
    weekend = proc[
        (proc["weekday"] >= 5) &
        proc["amount"].notna() &
        (proc["amount"] >= T6_WEEKEND_AMOUNT)
    ]
    for _, row in weekend.sort_values("amount", ascending=False).head(20).iterrows():
        findings.append({
            "rule": "weekend_award",
            "ada": row.get("ada"),
            "date": str(row["issue_date"])[:10] if pd.notna(row["issue_date"]) else None,
            "weekday": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][int(row["weekday"])],
            "amount": float(row["amount"]),
            "supplier": str(row.get("supplier_name", "")) or None,
            "subject": str(row.get("subject", ""))[:100],
            "decision_type": str(row.get("decision_type", "")) or None,
        })

    # ── T6c: public holiday awards ──
    holiday_df = proc[proc["mmdd"].isin(GREEK_HOLIDAYS) & proc["amount"].notna() & (proc["amount"] >= T6_WEEKEND_AMOUNT)]
    for _, row in holiday_df.sort_values("amount", ascending=False).head(10).iterrows():
        findings.append({
            "rule": "holiday_award",
            "ada": row.get("ada"),
            "date": str(row["issue_date"])[:10] if pd.notna(row["issue_date"]) else None,
            "amount": float(row["amount"]),
            "subject": str(row.get("subject", ""))[:100],
            "decision_type": str(row.get("decision_type", "")) or None,
        })

    # ── T6d: monthly spike ──
    monthly = proc.groupby(["year", "month"]).size().reset_index(name="count")
    monthly["rolling_mean"] = monthly.groupby("year")["count"].transform("mean")
    monthly["spike_ratio"] = monthly["count"] / monthly["rolling_mean"]
    spikes = monthly[monthly["spike_ratio"] >= T6_MONTHLY_MULTIPLIER]
    for _, row in spikes.iterrows():
        findings.append({
            "rule": "monthly_spike",
            "year": int(row["year"]),
            "month": int(row["month"]),
            "decisions": int(row["count"]),
            "monthly_mean": round(float(row["rolling_mean"]), 1),
            "spike_ratio": round(float(row["spike_ratio"]), 1),
            "threshold": T6_MONTHLY_MULTIPLIER,
        })

    return {
        "tenet": "T6",
        "name": "Temporal clustering",
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── T8 · Single-source monopoly ──────────────────────────────────────────────

def detect_t8(df: pd.DataFrame) -> dict:
    # T8 targets single-source DIRECT award monopoly — use DIRECT_AWARD_TYPES,
    # not PROCUREMENT_TYPES, so that competitive tender finalisations (ΚΑΤΑΚΥΡΩΣΗ)
    # and signed contracts (ΣΥΜΒΑΣΗ) do not trigger false-positive monopoly signals.
    awards = df[
        df["decision_type"].isin(DIRECT_AWARD_TYPES) &
        df["supplier_tax_id"].notna() &
        df["amount"].notna() &
        (df["amount"] > 0)
    ].copy()

    if awards.empty:
        return {
            "tenet": "T8",
            "name": "Single-source monopoly",
            "fired": False,
            "note": "No direct awards with both supplier_tax_id and amount available",
            "findings": [],
        }

    # Remove known data-quality artefact: some records have the supplier AFM
    # entered in the amount field (awardAmount == AFM numerically).
    # Exclude amounts numerically within 1% of supplier_tax_id when amount > €1M.
    afm_numeric = pd.to_numeric(awards["supplier_tax_id"], errors="coerce")
    afm_as_amount = (
        awards["amount"].notna() &
        afm_numeric.notna() &
        (awards["amount"] > 1_000_000) &
        (((awards["amount"] - afm_numeric).abs() / afm_numeric.clip(lower=1)) < 0.01)
    )
    if afm_as_amount.any():
        n_excluded = int(afm_as_amount.sum())
        awards = awards[~afm_as_amount]
        # Store for reporting
        _t8_afm_excluded = n_excluded
    else:
        _t8_afm_excluded = 0

    if awards.empty:
        return {
            "tenet": "T8",
            "name": "Single-source monopoly",
            "fired": False,
            "note": "No valid direct awards after data-quality filtering",
            "findings": [],
        }

    yearly = (
        awards.groupby(["year", "supplier_tax_id"])
        .agg(total=("amount", "sum"), count=("amount", "count"), name=("supplier_name", "first"))
        .reset_index()
    )
    year_totals = awards.groupby("year")["amount"].sum().rename("year_total")
    yearly = yearly.join(year_totals, on="year")
    yearly["share"] = yearly["total"] / yearly["year_total"]

    findings = []

    # Rule 1: critical single-year dominance (>70%)
    critical = yearly[yearly["share"] >= T8_SHARE_CRITICAL]
    for _, row in critical.iterrows():
        findings.append({
            "rule": "critical_dominance",
            "supplier_tax_id": str(row["supplier_tax_id"]),
            "supplier_name": str(row["name"]) if pd.notna(row["name"]) else None,
            "year": int(row["year"]),
            "total": float(row["total"]),
            "share": round(float(row["share"]), 3),
            "contracts": int(row["count"]),
            "threshold": T8_SHARE_CRITICAL,
        })

    # Rule 2: sustained dominance (>50% for 2+ consecutive years)
    above50 = yearly[yearly["share"] >= T8_SHARE_THRESHOLD].copy()
    for supplier_id, grp in above50.groupby("supplier_tax_id"):
        years_above = sorted(grp["year"].tolist())
        # Find consecutive runs
        runs = []
        run = [years_above[0]]
        for y in years_above[1:]:
            if y == run[-1] + 1:
                run.append(y)
            else:
                runs.append(run)
                run = [y]
        runs.append(run)

        for run in runs:
            if len(run) >= T8_MIN_YEARS:
                run_data = grp[grp["year"].isin(run)]
                findings.append({
                    "rule": "sustained_monopoly",
                    "supplier_tax_id": str(supplier_id),
                    "supplier_name": str(run_data["name"].iloc[0]) if pd.notna(run_data["name"].iloc[0]) else None,
                    "years": run,
                    "avg_share": round(float(run_data["share"].mean()), 3),
                    "total_over_period": float(run_data["total"].sum()),
                    "threshold": T8_SHARE_THRESHOLD,
                    "min_years": T8_MIN_YEARS,
                    "year_by_year": [
                        {"year": int(r["year"]), "share": round(float(r["share"]), 3), "total": float(r["total"])}
                        for _, r in run_data.iterrows()
                    ],
                })

    result = {
        "tenet": "T8",
        "name": "Single-source monopoly",
        "years_analysed": sorted(awards["year"].dropna().astype(int).unique().tolist()),
        "suppliers_with_data": int(awards["supplier_tax_id"].nunique()),
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }
    if _t8_afm_excluded:
        result["data_quality_note"] = f"{_t8_afm_excluded} record(s) excluded: amount == supplier AFM (data entry error)"
    return result


# ── T1 · Direct award concentration (rolling 3-year) ─────────────────────────

def detect_t1(df: pd.DataFrame) -> dict:
    """T1: Supplier capturing >40% of org's direct awards (count) or >35% of spend
    over the most recent 3 full years of data.  Uses rolling window so a captured
    relationship that spans years isn't diluted by T8's per-year view.
    """
    # T1 is specifically about DIRECT awards — exclude competitive tender awards
    awards = df[
        df["decision_type"].isin(DIRECT_AWARD_TYPES) &
        df["supplier_tax_id"].notna() &
        df["issue_date"].notna()
    ].copy()

    # Remove known data-quality artefact: some records have the supplier AFM
    # entered in the amount field (awardAmount == AFM numerically).
    # Filter: exclude amounts > €30M (unrealistic for direct awards at this org scale)
    # and amounts numerically within 1% of the supplier_tax_id value (AFM-as-amount).
    if "amount" in awards.columns:
        awards["amount"] = pd.to_numeric(awards["amount"], errors="coerce")
        afm_numeric = pd.to_numeric(awards["supplier_tax_id"], errors="coerce")
        afm_as_amount = (
            awards["amount"].notna() &
            afm_numeric.notna() &
            (awards["amount"] > 1_000_000) &
            (((awards["amount"] - afm_numeric).abs() / afm_numeric.clip(lower=1)) < 0.01)
        )
        awards = awards[~afm_as_amount]

    # Exclude self-referential tax IDs: when the "supplier" tax_id appears in
    # >50% of ALL procurement decisions (not just filtered), it is almost certainly
    # the contracting authority's own VAT being mis-recorded as the supplier.
    all_procurement = df[df["decision_type"].isin(DIRECT_AWARD_TYPES)]["supplier_tax_id"].dropna()
    if len(all_procurement) > 0:
        all_counts = all_procurement.value_counts()
        self_ids = all_counts[all_counts / len(all_procurement) > 0.50].index
        awards = awards[~awards["supplier_tax_id"].isin(self_ids)]

    if awards.empty:
        return {"tenet": "T1", "name": "Direct award concentration", "fired": False,
                "note": "No direct award decisions with supplier_tax_id", "findings": []}

    # Determine the 3-year window — use all available data (full history gives
    # the clearest picture of sustained concentration). Rolling 3-year is
    # applied by using all years that have data, giving up to 7 years.
    # For orgs with only sparse years, use everything.
    year_counts = awards["year"].dropna().astype(int).value_counts()
    if year_counts.empty:
        return {"tenet": "T1", "name": "Direct award concentration", "fired": False,
                "note": "No year data", "findings": []}

    # Use ALL years that have at least 5 decisions with supplier_tax_id
    # (a 3-year rolling window is unreliable when coverage is patchy)
    window_years = sorted(year_counts[year_counts >= 5].index.tolist())
    if not window_years:
        window_years = sorted(year_counts.index.tolist())
    window = awards[awards["year"].isin(window_years)].copy()

    if window.empty:
        return {"tenet": "T1", "name": "Direct award concentration", "fired": False,
                "note": f"No data in window {window_years}", "findings": []}

    total_count = len(window)
    total_spend = window["amount"].dropna()[window["amount"] > 0].sum() if "amount" in window.columns else 0

    by_sup = (
        window.groupby("supplier_tax_id")
        .agg(
            count=("ada", "count"),
            spend=("amount", lambda x: pd.to_numeric(x, errors="coerce").clip(lower=0).sum()),
            name=("supplier_name", "first"),
            years=("year", lambda x: sorted(x.dropna().astype(int).unique().tolist())),
        )
        .reset_index()
    )

    findings = []
    for _, row in by_sup.iterrows():
        count_share = row["count"] / total_count if total_count else 0
        spend_share = row["spend"] / total_spend if total_spend > 0 else 0

        if row["count"] < T1_MIN_AWARDS:
            continue

        rule_fired = count_share >= T1_COUNT_THRESHOLD or spend_share >= T1_AMOUNT_THRESHOLD
        if rule_fired:
            findings.append({
                "rule": "direct_award_concentration",
                "supplier_tax_id": str(row["supplier_tax_id"]),
                "supplier_name": str(row["name"]) if pd.notna(row["name"]) else None,
                "window_years": window_years,
                "award_count": int(row["count"]),
                "award_count_share": round(float(count_share), 3),
                "spend": float(row["spend"]),
                "spend_share": round(float(spend_share), 3),
                "years_active": row["years"],
            })

    findings.sort(key=lambda x: -x["award_count_share"])

    return {
        "tenet": "T1",
        "name": "Direct award concentration",
        "window_years": window_years,
        "total_window_awards": int(total_count),
        "total_window_spend": float(total_spend),
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── T9A · Committee capture ───────────────────────────────────────────────────

def detect_t9a(df: pd.DataFrame) -> dict:
    awards = df[is_procurement(df)].copy()
    total = len(awards)
    if total == 0:
        return {"tenet": "T9A", "name": "Committee capture", "fired": False, "findings": []}

    # Extract committee/body name from subject — look for "ΕΠΙΤΡΟΠΗ" or "ΕΠΙΤΡΟΠ"
    awards = awards.copy()
    awards["_body"] = awards["subject"].str.extract(
        r"(ΕΠΙΤΡΟΠ[ΗΣ\w\s]{0,40}?(?:ΣΥΝΤΗΡ|ΑΝΑΘΕΣ|ΕΚΤΕΛ|ΔΙΑΧΕΙΡ|ΑΞΙΟΛΟΓ)[Α-Ωα-ω\s]{0,30})",
        expand=False
    ).str.strip().str.replace(r"\s+", " ", regex=True).str[:60]  # collapse newlines/spaces

    body_counts = awards["_body"].value_counts()
    findings = []
    for body, count in body_counts.items():
        if not body or pd.isna(body):
            continue
        share = count / total
        if share >= T9A_THRESHOLD:
            findings.append({
                "rule": "committee_capture",
                "body": body,
                "direct_awards": int(count),
                "total_awards": int(total),
                "share": round(share, 3),
                "threshold": T9A_THRESHOLD,
            })

    return {
        "tenet": "T9A",
        "name": "Committee capture",
        "total_direct_awards": int(total),
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── T9B · Copy-paste structured award ────────────────────────────────────────

def detect_t9b(df: pd.DataFrame) -> dict:
    awards = df[
        is_procurement(df) &
        df["amount"].notna() &
        (df["amount"] > 0) &
        df["supplier_tax_id"].notna()
    ].copy()

    # Non-round amounts only
    awards["_non_round"] = awards["amount"] % T9B_ROUND_DIVISOR != 0
    awards = awards[awards["_non_round"]]

    grouped = (
        awards.groupby(["supplier_tax_id", "amount"])
        .agg(count=("ada", "count"), name=("supplier_name", "first"),
             dates=("issue_date", lambda x: sorted(str(v)[:10] for v in x if pd.notna(v))))
        .reset_index()
    )
    hits = grouped[grouped["count"] >= T9B_MIN_COUNT].sort_values("count", ascending=False)

    findings = []
    for _, row in hits.iterrows():
        findings.append({
            "rule": "copy_paste_amount",
            "supplier_tax_id": str(row["supplier_tax_id"]),
            "supplier_name": str(row["name"]) if pd.notna(row["name"]) else None,
            "amount": float(row["amount"]),
            "occurrences": int(row["count"]),
            "dates": row["dates"][:5],
        })

    return {
        "tenet": "T9B",
        "name": "Copy-paste structured award",
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── T9C · 30-day award burst ──────────────────────────────────────────────────

def detect_t9c(df: pd.DataFrame) -> dict:
    # Only direct awards — ΚΑΤΑΚΥΡΩΣΗ (competitive tender finalization) is
    # legitimate and should NOT be flagged as a burst.
    awards = df[
        df["decision_type"].isin(DIRECT_AWARD_TYPES) &
        df["supplier_tax_id"].notna() &
        df["issue_date"].notna()
    ].copy()
    awards = awards.sort_values(["supplier_tax_id", "issue_date"])

    findings = []
    for supplier_id, grp in awards.groupby("supplier_tax_id"):
        grp = grp.sort_values("issue_date").reset_index(drop=True)
        if len(grp) < T9C_MIN_AWARDS:
            continue
        # Sliding window
        dates = grp["issue_date"].tolist()
        for i in range(len(dates)):
            window = [j for j in range(i, len(dates))
                      if (dates[j] - dates[i]).days <= T9C_WINDOW_DAYS]
            if len(window) >= T9C_MIN_AWARDS:
                window_rows = grp.iloc[window]
                total_amt = pd.to_numeric(window_rows["amount"], errors="coerce").sum()
                findings.append({
                    "rule": "burst_window",
                    "supplier_tax_id": str(supplier_id),
                    "supplier_name": str(grp["supplier_name"].iloc[0]) if pd.notna(grp["supplier_name"].iloc[0]) else None,
                    "awards_in_window": len(window),
                    "window_start": str(dates[i])[:10],
                    "window_end": str(dates[window[-1]])[:10],
                    "total_amount": float(total_amt) if total_amt == total_amt else None,
                    "subjects": window_rows["subject"].dropna().str[:60].tolist()[:3],
                })
                break  # one finding per supplier (the biggest burst)

    # Deduplicate by supplier
    seen = set()
    deduped = []
    for f in sorted(findings, key=lambda x: -x["awards_in_window"]):
        if f["supplier_tax_id"] not in seen:
            seen.add(f["supplier_tax_id"])
            deduped.append(f)

    return {
        "tenet": "T9C",
        "name": "30-day award burst",
        "fired": len(deduped) > 0,
        "findings": clean(deduped),
    }


# ── T9D · Price standardisation across suppliers ──────────────────────────────

def detect_t9d(df: pd.DataFrame) -> dict:
    awards = df[
        is_procurement(df) &
        df["amount"].notna() &
        (df["amount"] > 0) &
        df["supplier_tax_id"].notna()
    ].copy()

    awards["_non_round"] = awards["amount"] % T9B_ROUND_DIVISOR != 0
    awards = awards[awards["_non_round"]]

    grouped = (
        awards.groupby("amount")
        .agg(supplier_count=("supplier_tax_id", "nunique"),
             total_count=("ada", "count"),
             suppliers=("supplier_name", lambda x: list(x.dropna().unique())[:5]))
        .reset_index()
    )
    hits = grouped[grouped["supplier_count"] >= T9D_MIN_SUPPLIERS].sort_values(
        "supplier_count", ascending=False
    )

    findings = []
    for _, row in hits.iterrows():
        findings.append({
            "rule": "price_standardisation",
            "amount": float(row["amount"]),
            "distinct_suppliers": int(row["supplier_count"]),
            "total_occurrences": int(row["total_count"]),
            "suppliers": row["suppliers"],
            "threshold": T9D_MIN_SUPPLIERS,
        })

    return {
        "tenet": "T9D",
        "name": "Price standardisation across suppliers",
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

DETECTORS = {
    "T1": detect_t1,
    "T4": detect_t4,
    "T5": detect_t5,
    "T6": detect_t6,
    "T8": detect_t8,
    "T9A": detect_t9a,
    "T9B": detect_t9b,
    "T9C": detect_t9c,
    "T9D": detect_t9d,
}


def print_report(results: list[dict]) -> None:
    for r in results:
        fired = "🔴 FIRED" if r.get("fired") else "🟢 clean"
        print(f"\n{'='*60}")
        print(f"{r['tenet']} · {r['name']}  {fired}")
        print(f"{'='*60}")

        for k, v in r.items():
            if k in ("tenet", "name", "fired", "findings"):
                continue
            print(f"  {k}: {v}")

        findings = r.get("findings", [])
        if not findings:
            print("  No findings.")
            continue

        print(f"\n  Findings ({len(findings)}):")
        for f in findings:
            rule = f.get("rule", "")
            if rule == "direct_award_concentration":
                name = f.get("supplier_name") or f"AFM {f['supplier_tax_id']}"
                print(f"    {name} — {f['award_count']} awards "
                      f"({f['award_count_share']*100:.0f}% of count, "
                      f"{f['spend_share']*100:.0f}% of spend) "
                      f"in {f['window_years']}")
            elif rule == "annual_rate":
                print(f"    [{f['year']}] Emergency rate {f['rate']*100:.1f}% "
                      f"({f['emergency_decisions']}/{f['total_procurement_decisions']} decisions) "
                      f"— threshold {f['threshold']*100:.0f}%")
            elif rule == "high_value_emergency":
                print(f"    [{f.get('year','')}] High-value emergency award {fmt_eur(f['amount'])} "
                      f"— {f['subject'][:70]}")
            elif rule == "yearend_burst":
                print(f"    [{f['year']}] Dec 22-31: {f['yearend_decisions']} decisions "
                      f"({f['multiplier']}× baseline daily rate)")
            elif rule == "weekend_award":
                print(f"    [{f.get('date','')} {f.get('weekday','')}] "
                      f"{fmt_eur(f['amount'])} — {f.get('supplier','?')} — {f['subject'][:60]}")
            elif rule == "holiday_award":
                print(f"    [{f.get('date','')} HOLIDAY] {fmt_eur(f['amount'])} — {f['subject'][:70]}")
            elif rule == "monthly_spike":
                print(f"    [{f['year']}-{f['month']:02d}] {f['decisions']} decisions "
                      f"({f['spike_ratio']}× monthly mean {f['monthly_mean']:.0f})")
            elif rule == "critical_dominance":
                print(f"    [{f['year']}] {f['supplier_name']} (AFM {f['supplier_tax_id']}) "
                      f"= {f['share']*100:.0f}% of direct-award spend ({fmt_eur(f['total'])})")
            elif rule == "sustained_monopoly":
                print(f"    {f['supplier_name']} (AFM {f['supplier_tax_id']}) "
                      f"years {f['years']} avg {f['avg_share']*100:.0f}% "
                      f"total {fmt_eur(f['total_over_period'])}")
            elif rule == "committee_capture":
                print(f"    {f['body'][:55]}")
                print(f"      → {f['direct_awards']}/{f['total_awards']} awards ({f['share']*100:.0f}%)")
            elif rule == "copy_paste_amount":
                print(f"    {f['supplier_name']} (AFM {f['supplier_tax_id']}) "
                      f"— €{f['amount']:,.2f} × {f['occurrences']} times  dates: {f['dates']}")
            elif rule == "burst_window":
                print(f"    {f['supplier_name']} (AFM {f['supplier_tax_id']}) "
                      f"— {f['awards_in_window']} awards {f['window_start']}→{f['window_end']} "
                      f"total {fmt_eur(f['total_amount']) if f['total_amount'] else '?'}")
            elif rule == "price_standardisation":
                print(f"    €{f['amount']:,.2f} paid to {f['distinct_suppliers']} different suppliers "
                      f"({f['total_occurrences']} total occurrences)")
                for s in f['suppliers'][:3]:
                    print(f"      · {s}")
            elif rule in ("amendment_inflation", "price_carry"):
                sup = f.get("supplier_name") or f"AFM {f['supplier_tax_id']}"
                label = "AMENDMENT INFLATION" if rule == "amendment_inflation" else "price carry"
                print(f"    [{label}] {sup}")
                n  = f["contract_n"]
                n1 = f["contract_n1"]
                print(f"      Contract N  {n['date']}  {fmt_eur(n['amount'])}  {n['subject']}")
                if f.get("amendment_ada"):
                    print(f"      Amendment   {f['amendment_ada']}  "
                          f"{fmt_eur(f['amendment_amount']) if f.get('amendment_amount') else '?'}")
                print(f"      Contract N+1 {n1['date']}  {fmt_eur(n1['amount'])}  {n1['subject']}")
                print(f"      gap {f['gap_days']}d  Δ {f['pct_diff']:.1f}%")
            else:
                print(f"    {f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Procurement signal detector")
    parser.add_argument("--org", required=True, help="Organisation UID")
    parser.add_argument("--tenets", default="T1,T4,T5,T6,T8,T9A,T9B,T9C,T9D",
                        help="Comma-separated list of tenets to run (default: all)")
    parser.add_argument("--json", action="store_true", dest="output_json",
                        help="Output machine-readable JSON")
    args = parser.parse_args()

    tenets = [t.strip().upper() for t in args.tenets.split(",")]
    unknown = [t for t in tenets if t not in DETECTORS]
    if unknown:
        sys.exit(f"Unknown tenets: {unknown}. Available: {list(DETECTORS)}")

    df = load_decisions(args.org)
    results = [DETECTORS[t](df) for t in tenets]

    if args.output_json:
        print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"\nSignal detection — org {args.org}")
        print(f"Total decisions loaded: {len(df):,}")
        print_report(results)


if __name__ == "__main__":
    main()
