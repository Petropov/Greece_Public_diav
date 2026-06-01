#!/usr/bin/env python3
"""Signal detectors for procurement intelligence tenets.

Implements:
  T5 · Emergency procurement overuse
  T6 · Temporal clustering (year-end burst, weekend/holiday awards)
  T8 · Single-source monopoly

Usage:
    python scripts/detect_signals.py --org 6166
    python scripts/detect_signals.py --org 6166 --json          # machine-readable output
    python scripts/detect_signals.py --org 6166 --tenets T5,T8  # run specific tenets

Rules of the game
-----------------
T5  A procurement decision is "emergency" if its subject contains any of the
    emergency keywords (see T5_KEYWORDS). An org FIRES T5 if emergency decisions
    exceed T5_THRESHOLD (15%) of all procurement-type decisions in any calendar year,
    OR if any single emergency award exceeds T5_HIGH_VALUE (€60k — double the
    direct-award ceiling, indicating the emergency bypass was used to avoid tendering
    a contract that clearly required it).

T6a Year-end burst. For each calendar year, compute the daily award rate in
    Dec 22–31 vs the daily rate for Jan 1–Dec 21. FIRES if the year-end rate
    exceeds T6_YEAREND_MULTIPLIER (3×) the baseline rate. (Dec already runs 2× —
    3× is the anomaly threshold.)

T6b Weekend awards. Flag awards issued on Saturday or Sunday where the amount
    exceeds T6_WEEKEND_AMOUNT (€5k). Low volume but high signal — legitimate
    emergency awards on weekends exist but are rare.

T6c Monthly spike. Flag any calendar month where the decision count exceeds
    T6_MONTHLY_MULTIPLIER (2.5×) the 12-month rolling average for that org.

T8  For each (org, year), compute each supplier's share of total direct-award
    spend. FIRES if a single supplier exceeds T8_SHARE_THRESHOLD (50%) of the
    year's direct-award spend AND the relationship persists for at least
    T8_MIN_YEARS (2) consecutive years. Secondary flag: any supplier >70% in
    any single year regardless of persistence.
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

T5_KEYWORDS = (
    "διαπραγμάτευση χωρίς προηγούμενη δημοσίευση",
    "διαπραγματευση χωρις προηγουμενη δημοσιευση",
    "αδυναμία ανταγωνισμού",
    "αδυναμια ανταγωνισμου",
    "μοναδικός προμηθευτής",
    "μοναδικος προμηθευτης",
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
T6_WEEKEND_AMOUNT = 5_000  # €5k minimum for a weekend award to be flagged
T6_MONTHLY_MULTIPLIER = 2.5

T8_SHARE_THRESHOLD = 0.50  # 50% of annual direct-award spend
T8_SHARE_CRITICAL = 0.70   # 70% — fire regardless of persistence
T8_MIN_YEARS = 2           # consecutive years above threshold

PROCUREMENT_TYPES = {
    "ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ",
    "ΣΥΜΒΑΣΗ",
    "ΚΑΤΑΚΥΡΩΣΗ",
    "Procurement assignment",
    "Contract award",
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
    findings = []

    # ── T6a: year-end burst ──
    for year, grp in df.groupby("year"):
        grp = grp[grp["issue_date"].notna()].copy()
        if len(grp) < 30:
            continue

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
    weekend = df[
        (df["weekday"] >= 5) &
        df["amount"].notna() &
        (df["amount"] >= T6_WEEKEND_AMOUNT)
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
        })

    # ── T6c: public holiday awards ──
    holiday_df = df[df["mmdd"].isin(GREEK_HOLIDAYS) & df["amount"].notna() & (df["amount"] >= T6_WEEKEND_AMOUNT)]
    for _, row in holiday_df.sort_values("amount", ascending=False).head(10).iterrows():
        findings.append({
            "rule": "holiday_award",
            "ada": row.get("ada"),
            "date": str(row["issue_date"])[:10] if pd.notna(row["issue_date"]) else None,
            "amount": float(row["amount"]),
            "subject": str(row.get("subject", ""))[:100],
        })

    # ── T6d: monthly spike ──
    monthly = df.groupby(["year", "month"]).size().reset_index(name="count")
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
    awards = df[
        is_procurement(df) &
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

    return {
        "tenet": "T8",
        "name": "Single-source monopoly",
        "years_analysed": sorted(awards["year"].dropna().astype(int).unique().tolist()),
        "suppliers_with_data": int(awards["supplier_tax_id"].nunique()),
        "fired": len(findings) > 0,
        "findings": clean(findings),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

DETECTORS = {
    "T5": detect_t5,
    "T6": detect_t6,
    "T8": detect_t8,
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
            if rule == "annual_rate":
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
            else:
                print(f"    {f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Procurement signal detector")
    parser.add_argument("--org", required=True, help="Organisation UID")
    parser.add_argument("--tenets", default="T5,T6,T8",
                        help="Comma-separated list of tenets to run (default: T5,T6,T8)")
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
