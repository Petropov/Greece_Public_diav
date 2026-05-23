#!/usr/bin/env python3
"""
Build a like-for-like comparison report for:
  • ΓΝ Λαμίας  vs  ΓΝ Τρικάλων / ΓΝ Καρδίτσας / ΓΝ Κοζάνης / ΓΝ Σερρών
  • ΔΕΥΑ Λαμίας vs  ΔΕΥΑ Τρικάλων / ΔΕΥΑ Κοζάνης / ΔΕΥΑ Σερρών / ΔΕΥΑ Καρδίτσας

Usage:
    python scripts/build_hospital_deyva_comparison.py
    python scripts/build_hospital_deyva_comparison.py --output reports/hospital_deyva_comparison.md
"""
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

REPO = Path(__file__).resolve().parents[1]
NORM = REPO / "data" / "normalized"

# ── Entity definitions ─────────────────────────────────────────────────────────

class Entity(NamedTuple):
    org: str
    name: str
    short: str
    population: int | None  # approximate catchment / served population

HOSPITALS = [
    Entity("99221923", "ΓΝ Λαμίας",             "Λαμία",    75_000),
    Entity("99221946", "ΓΝ Τρικάλων",            "Τρίκαλα",  81_000),
    Entity("99221913", "ΓΝ Καρδίτσας",           "Καρδίτσα", 58_000),
    Entity("99221920", "ΓΝ Κοζάνης (Μαμάτσειο)", "Κοζάνη",   47_000),
    Entity("99221942", "ΓΝ Σερρών",              "Σέρρες",   57_000),
]

DEYVAS = [
    Entity("50304", "ΔΕΥΑ Λαμίας",    "Λαμία",    75_000),
    Entity("51546", "ΔΕΥΑ Τρικάλων",  "Τρίκαλα",  81_000),
    Entity("50432", "ΔΕΥΑ Καρδίτσας", "Καρδίτσα", 58_000),
    Entity("50449", "ΔΕΥΑ Κοζάνης",   "Κοζάνη",   47_000),
    Entity("50621", "ΔΕΥΑ Σερρών",    "Σέρρες",   57_000),
]

# Decision type label fragments
DA_LABEL   = "ΑΝΑΘΕΣΗ"          # direct award
COMP_LABEL = "ΚΑΤΑΚΥΡΩΣΗ"       # competitive award
CONT_LABEL = "ΣΥΜΒΑΣΗ"          # contract
BUDG_LABEL = "ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ"
PAY_LABEL  = "ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ"
EGKR_LABEL = "ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ"

# ── Data loading ───────────────────────────────────────────────────────────────

def load_decisions(org: str) -> list[dict]:
    path = NORM / f"org={org}" / "decisions.csv"
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def safe_float(v: str) -> float | None:
    try:
        return float(v) if v and v.strip() else None
    except ValueError:
        return None


def org_stats(org: str) -> dict:
    rows = load_decisions(org)
    if not rows:
        return {}

    total = len(rows)
    by_type: Counter = Counter(r.get("decision_type", "") for r in rows)
    by_year: Counter = Counter(int(r["year"]) for r in rows if r.get("year"))

    # Procurement categories
    da_rows   = [r for r in rows if DA_LABEL   in r.get("decision_type", "")]
    comp_rows = [r for r in rows if COMP_LABEL in r.get("decision_type", "")]
    cont_rows = [r for r in rows if CONT_LABEL in r.get("decision_type", "")]

    def amt_stats(subset: list[dict]) -> tuple[int, float]:
        amounts = [safe_float(r.get("amount", "")) for r in subset]
        amounts = [a for a in amounts if a is not None]
        return len(amounts), sum(amounts)

    da_n,   da_sum   = amt_stats(da_rows)
    comp_n, comp_sum = amt_stats(comp_rows)
    cont_n, cont_sum = amt_stats(cont_rows)

    # total known spend — restrict to procurement-relevant types to avoid
    # double-counting budget commitments (ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ) vs actual awards
    PROCUREMENT_LABELS = (
        DA_LABEL, COMP_LABEL, CONT_LABEL,
        "ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ ΚΑΙ ΑΝΑΘΕΣΗ",  # Β.1.3 label variant
    )
    spend_rows = [
        r for r in rows
        if any(lbl in r.get("decision_type", "") for lbl in PROCUREMENT_LABELS)
    ]
    all_amts = [safe_float(r.get("amount", "")) for r in spend_rows]
    all_amts = [a for a in all_amts if a is not None]

    # Year breakdown for direct awards
    da_by_year: dict[int, tuple[int,int,float]] = {}
    for yr in sorted(by_year.keys()):
        yr_rows = [r for r in rows if r.get("year") and int(r["year"]) == yr]
        yr_da   = [r for r in yr_rows if DA_LABEL in r.get("decision_type","")]
        yr_da_n, yr_da_sum = amt_stats(yr_da)
        da_by_year[yr] = (len(yr_rows), len(yr_da), yr_da_sum)

    return {
        "total": total,
        "by_type": by_type,
        "by_year": by_year,
        "da_count": len(da_rows),
        "da_amount_n": da_n,
        "da_amount": da_sum,
        "comp_count": len(comp_rows),
        "comp_amount_n": comp_n,
        "comp_amount": comp_sum,
        "cont_count": len(cont_rows),
        "cont_amount_n": cont_n,
        "cont_amount": cont_sum,
        "all_amount_n": len(all_amts),
        "all_amount": sum(all_amts),
        "da_by_year": da_by_year,
    }


# ── Formatting helpers ─────────────────────────────────────────────────────────

def pct(num: int, den: int) -> str:
    if den == 0:
        return "—"
    return f"{100*num/den:.1f}%"


def eur(v: float | None, decimals: int = 1) -> str:
    if v is None or v == 0:
        return "—"
    if v >= 1_000_000:
        return f"€{v/1_000_000:.{decimals}f}M"
    if v >= 1_000:
        return f"€{v/1_000:.0f}K"
    return f"€{v:.0f}"


def cov(n: int, total: int) -> str:
    if total == 0:
        return "—"
    return f"{pct(n,total)} κάλυψη"


def mk_row(entity: Entity, s: dict, lamia_s: dict | None = None) -> str:
    if not s:
        return f"| {entity.name} | ❌ no data | — | — | — | — | — |"

    da_eur  = eur(s["da_amount"])
    cmp_eur = eur(s["comp_amount"])
    total_eur = eur(s["all_amount"])
    da_pct_count = pct(s["da_count"], s["total"])
    da_pct_spend = pct(int(s["da_amount"]), int(s["all_amount"])) if s["all_amount"] else "—"

    star = " ⭐" if entity.org in ("99221923","50304") else ""
    return (
        f"| **{entity.name}**{star} "
        f"| {s['total']:,} "
        f"| {s['da_count']:,} ({da_pct_count}) "
        f"| {s['comp_count']:,} "
        f"| {da_eur} ({cov(s['da_amount_n'], s['da_count'])}) "
        f"| {total_eur} "
        f"| {da_pct_spend} |"
    )


# ── Report builder ─────────────────────────────────────────────────────────────

def build_report(today: str) -> str:
    lines: list[str] = []
    a = lines.append

    a(f"# Νοσοκομεία & ΔΕΥΑ — Συγκριτική Ανάλυση")
    a(f"*Περίοδος: 2020–2026 · Πηγή: Διαύγεια API · Ημερομηνία: {today}*")
    a("")
    a("---")
    a("")
    a("## Μεθοδολογία")
    a("")
    a("- Δεδομένα: search exports + στοχευμένη hydration (τύποι Δ.1 · Δ.2.2 · Γ.3.4 · Β.1.3)")
    a("- Ποσά: διαθέσιμα μόνο για αποφάσεις που υποβλήθηκαν σε hydration — η κάλυψη αναφέρεται ρητά")
    a("- «Άμεσες Αναθέσεις»: αποφάσεις τύπου ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ (Δ.1)")
    a("- «Ανταγωνιστικές»: ΚΑΤΑΚΥΡΩΣΗ (Δ.2.2) — κατακύρωση μετά από ανοιχτό ή κλειστό διαγωνισμό")
    a("- ⚠️ Ορισμένα νοσοκομεία χρησιμοποιούν ΒΧ τύπους (ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ / ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ)")
    a("  αντί Δ.1 για άμεσες αναθέσεις — το ποσοστό ΑΝΑΘΕΣΗ για αυτά είναι τεχνητά χαμηλό")
    a("")

    # ── HOSPITALS ──────────────────────────────────────────────────────────────
    a("---")
    a("")
    a("## 1. Νοσοκομεία")
    a("")

    h_stats = {e.org: org_stats(e.org) for e in HOSPITALS}

    a("### 1.1 Αποφάσεις & Άμεσες Αναθέσεις (2020–2026)")
    a("")
    a("| Νοσοκομείο | Σύνολο Αποφ. | ΑΝΑΘΕΣΗ (Δ.1) | ΚΑΤΑΚΥΡΩΣΗ | ΑΝΑΘΕΣΗ € (κάλυψη) | Γνωστή Δαπάνη | ΑΝΑΘΕΣΗ % δαπάνης |")
    a("|------------|-------------|---------------|-----------|---------------------|---------------|-------------------|")
    for e in HOSPITALS:
        a(mk_row(e, h_stats.get(e.org, {})))
    a("")
    a("⭐ = οντότητα αναφοράς (Λαμία)")
    a("")

    # Narrative analysis for hospitals
    lamia_h = h_stats.get("99221923", {})
    a("### 1.2 Βασικά Ευρήματα — Νοσοκομεία")
    a("")
    if lamia_h:
        trik_h  = h_stats.get("99221946", {})
        serr_h  = h_stats.get("99221942", {})
        kard_h  = h_stats.get("99221913", {})
        koz_h   = h_stats.get("99221920", {})

        lamia_da  = lamia_h.get("da_count", 0)
        trik_da   = trik_h.get("da_count",  0)
        serr_da   = serr_h.get("da_count",  0)
        kard_da   = kard_h.get("da_count",  0)
        koz_da    = koz_h.get("da_count",   0)

        lamia_comp = lamia_h.get("comp_count", 0)
        trik_comp  = trik_h.get("comp_count",  0)
        serr_comp  = serr_h.get("comp_count",  0)

        peers_da_avg = (trik_da + serr_da + kard_da + koz_da) / 4 if any([trik_da,serr_da,kard_da,koz_da]) else 0
        mult = f"{lamia_da/peers_da_avg:.1f}×" if peers_da_avg else "πολλαπλάσιο"

        a(f"**Άμεσες Αναθέσεις (ΑΝΑΘΕΣΗ Δ.1):**")
        a(f"- ΓΝ Λαμίας: **{lamia_da:,}** αναθέσεις — {mult} πάνω από τον μ.ο. ομοτίμων ({peers_da_avg:.0f})")
        a(f"- ΓΝ Τρικάλων: {trik_da:,} | ΓΝ Σερρών: {serr_da:,} | ΓΝ Καρδίτσας: {kard_da:,} | ΓΝ Κοζάνης: {koz_da:,}")
        a("")
        a(f"**Ανταγωνιστικές Κατακυρώσεις (ΚΑΤΑΚΥΡΩΣΗ Δ.2.2):**")
        a(f"- ΓΝ Λαμίας: {lamia_comp:,} | ΓΝ Τρικάλων: {trik_comp:,} | ΓΝ Σερρών: {serr_comp:,}")
        a("")

        # Only show spend % when coverage is meaningful (≥10% of da decisions have amounts)
        def spend_pct_note(s: dict, name: str) -> str | None:
            da_n = s.get("da_amount_n", 0)
            da_count = s.get("da_count", 0)
            if da_count == 0 or da_n / da_count < 0.10:
                return None
            all_amt = s.get("all_amount", 0)
            if not all_amt:
                return None
            ratio = s["da_amount"] / all_amt * 100
            return f"- {name}: {eur(s['da_amount'])} = **{ratio:.1f}%** γνωστής δαπάνης ({cov(da_n, da_count)})"

        notes = [
            spend_pct_note(lamia_h, "ΓΝ Λαμίας"),
            spend_pct_note(trik_h,  "ΓΝ Τρικάλων"),
            spend_pct_note(serr_h,  "ΓΝ Σερρών"),
        ]
        notes = [n for n in notes if n]
        if notes:
            a(f"**Ποσοστό Άμεσης Ανάθεσης (Δ.1) ως % γνωστής συνολικής δαπάνης:**")
            for n in notes:
                a(n)
            a("> ⚠️ Τα ποσά για ΓΝ Τρικάλων / Καρδίτσα / Κοζάνη υποεκτιμούνται (hydration ακόμη σε εξέλιξη)")
            a("")

        a("**Σημείωση για ΓΝ Καρδίτσας & ΓΝ Κοζάνης:**")
        a("Αυτά τα νοσοκομεία χρησιμοποιούν Β-τύπους (ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ, ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ)")
        a("αντί Δ.1 για την τεκμηρίωση δαπανών. Το μηδενικό ΑΝΑΘΕΣΗ δεν σημαίνει απουσία")
        a("άμεσων αναθέσεων — αντικατοπτρίζει **διαφορετική διοικητική κωδικοποίηση**.")
        a("")

    # Year-by-year for hospitals
    a("### 1.3 Άμεσες Αναθέσεις ανά Έτος — Νοσοκομεία")
    a("")
    years = list(range(2020, 2027))
    header = "| Έτος |" + "".join(f" {e.short} |" for e in HOSPITALS)
    sep    = "|------|" + "".join("---------|" for _ in HOSPITALS)
    a(header)
    a(sep)
    for yr in years:
        row = f"| {yr} |"
        for e in HOSPITALS:
            s = h_stats.get(e.org, {})
            da_yr = s.get("da_by_year", {}).get(yr, (0, 0, 0.0))
            tot_yr, da_cnt, da_amt = da_yr
            if tot_yr == 0:
                row += " — |"
            else:
                row += f" {da_cnt} ({pct(da_cnt, tot_yr)}) |"
        a(row)
    a("")

    # ── DEYVAS ─────────────────────────────────────────────────────────────────
    a("---")
    a("")
    a("## 2. ΔΕΥΑ")
    a("")

    d_stats = {e.org: org_stats(e.org) for e in DEYVAS}

    a("### 2.1 Αποφάσεις & Άμεσες Αναθέσεις (2020–2026)")
    a("")
    a("| ΔΕΥΑ | Σύνολο Αποφ. | ΑΝΑΘΕΣΗ (Δ.1) | ΚΑΤΑΚΥΡΩΣΗ | ΑΝΑΘΕΣΗ € (κάλυψη) | Γνωστή Δαπάνη | ΑΝΑΘΕΣΗ % δαπάνης |")
    a("|------|-------------|---------------|-----------|---------------------|---------------|-------------------|")
    for e in DEYVAS:
        a(mk_row(e, d_stats.get(e.org, {})))
    a("")

    # Narrative for DEYAs
    lamia_d  = d_stats.get("50304",  {})
    trik_d   = d_stats.get("51546",  {})
    kard_d   = d_stats.get("50432",  {})
    koz_d    = d_stats.get("50449",  {})
    serr_d   = d_stats.get("50621",  {})

    a("### 2.2 Βασικά Ευρήματα — ΔΕΥΑ")
    a("")
    if lamia_d:
        da_lamia = lamia_d.get("da_count", 0)
        peers = [trik_d, kard_d, koz_d, serr_d]
        peer_da = [p.get("da_count", 0) for p in peers]
        peer_avg = sum(peer_da) / len([x for x in peer_da if x > 0]) if any(peer_da) else 0

        a(f"**ΔΕΥΑ Λαμίας: {da_lamia:,} άμεσες αναθέσεις**")
        a(f"- Μ.ο. ομοτίμων: {peer_avg:.0f} | Τρίκαλα: {trik_d.get('da_count',0):,} | Καρδίτσα: {kard_d.get('da_count',0):,} | Κοζάνη: {koz_d.get('da_count',0):,} | Σέρρες: {serr_d.get('da_count',0):,}")
        if peer_avg > 0:
            mult_d = da_lamia / peer_avg
            a(f"- ΔΕΥΑ Λαμίας = **{mult_d:.1f}× τον μ.ο.** ομοτίμων ΔΕΥΑ")
        a("")

        if lamia_d.get("da_amount"):
            a(f"**Ποσά Άμεσων Αναθέσεων (μετά hydration):**")
            a(f"- ΔΕΥΑ Λαμίας: {eur(lamia_d['da_amount'])} ({cov(lamia_d['da_amount_n'], da_lamia)})")
            for e, s in [(DEYVAS[1], trik_d), (DEYVAS[2], kard_d), (DEYVAS[3], koz_d), (DEYVAS[4], serr_d)]:
                if s.get("da_amount"):
                    a(f"- {e.name}: {eur(s['da_amount'])} ({cov(s['da_amount_n'], s['da_count'])})")
            a("")

    # Year-by-year for DEYAs
    a("### 2.3 Άμεσες Αναθέσεις ανά Έτος — ΔΕΥΑ")
    a("")
    header = "| Έτος |" + "".join(f" {e.short} |" for e in DEYVAS)
    sep    = "|------|" + "".join("---------|" for _ in DEYVAS)
    a(header)
    a(sep)
    for yr in years:
        row = f"| {yr} |"
        for e in DEYVAS:
            s = d_stats.get(e.org, {})
            da_yr = s.get("da_by_year", {}).get(yr, (0, 0, 0.0))
            tot_yr, da_cnt, da_amt = da_yr
            if tot_yr == 0:
                row += " — |"
            else:
                row += f" {da_cnt} ({pct(da_cnt, tot_yr)}) |"
        a(row)
    a("")

    # ── Cross-sector summary ───────────────────────────────────────────────────
    a("---")
    a("")
    a("## 3. Συνολική Εικόνα — Λαμία vs Ομότιμοι")
    a("")
    a("| Οντότητα | Τύπος | Άμεσες Αναθέσεις | % Αποφ. | Ανταγωνιστικές | Άμεσες/Ανταγ. |")
    a("|----------|-------|-----------------|---------|----------------|---------------|")

    for group, stats_map, label in [(HOSPITALS, h_stats, "Νοσοκομείο"), (DEYVAS, d_stats, "ΔΕΥΑ")]:
        for e in group:
            s = stats_map.get(e.org, {})
            if not s:
                continue
            da = s.get("da_count", 0)
            comp = s.get("comp_count", 0)
            ratio_str = f"{da/comp:.1f}×" if comp > 0 else f"{da}:0"
            flag = " ⭐" if e.org in ("99221923","50304") else ""
            a(f"| **{e.short}**{flag} | {label} | {da:,} | {pct(da, s['total'])} | {comp:,} | {ratio_str} |")
    a("")
    a("⭐ = Λαμία (αναφορά)")
    a("")

    a("---")
    a("")
    a("## 4. Συμπεράσματα")
    a("")
    a("### Νοσοκομεία")
    a("")
    lamia_h_da   = h_stats.get("99221923",{}).get("da_count",0)
    lamia_h_comp = h_stats.get("99221923",{}).get("comp_count",0)
    trik_h_da    = h_stats.get("99221946",{}).get("da_count",0)
    trik_h_comp  = h_stats.get("99221946",{}).get("comp_count",0)

    if lamia_h_da and trik_h_da:
        ratio_da_nh = lamia_h_da / trik_h_da
        a(f"1. **ΓΝ Λαμίας αναθέτει {ratio_da_nh:.1f}× περισσότερα απευθείας** από το ΓΝ Τρικάλων ({lamia_h_da:,} vs {trik_h_da:,})")
    if lamia_h_comp and trik_h_comp:
        ratio_comp_nh = trik_h_comp / lamia_h_comp if lamia_h_comp else 0
        a(f"2. **ΓΝ Τρικάλων κάνει {ratio_comp_nh:.1f}× περισσότερες ανταγωνιστικές κατακυρώσεις** ({trik_h_comp:,} vs {lamia_h_comp:,})")
    a("3. ΓΝ Καρδίτσας & ΓΝ Κοζάνης χρησιμοποιούν Β-τύπους — η σύγκριση ΑΝΑΘΕΣΗ δεν είναι απευθείας")
    a("")
    a("### ΔΕΥΑ")
    a("")
    lamia_d_da = d_stats.get("50304",{}).get("da_count",0)
    peers_d_da = sum(d_stats.get(e.org,{}).get("da_count",0) for e in DEYVAS[1:])
    if lamia_d_da and peers_d_da:
        peer_avg_d = peers_d_da / 4
        mult_d2 = lamia_d_da / peer_avg_d
        a(f"1. **ΔΕΥΑ Λαμίας: {lamia_d_da:,} άμεσες** — {mult_d2:.1f}× τον μέσο ομοτίμων ({peer_avg_d:.0f})")
    serr_d_da = d_stats.get("50621",{}).get("da_count",0)
    if serr_d_da:
        a(f"2. Σέρρες: {serr_d_da:,} άμεσες — ο δεύτερος υψηλότερος, αξίζει διερεύνηση")
    a("3. Τρίκαλα / Καρδίτσα / Κοζάνη: σαφώς χαμηλότερα επίπεδα άμεσων αναθέσεων")
    a("")
    a("---")
    a("")
    a("*Δεδομένα: Διαύγεια OpenData API · Pipeline: search+hydrate · Αρχεία: `data/normalized/org=*/`*")

    return "\n".join(lines)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path,
        default=REPO / "reports" / "hospital_deyva_comparison.md",
    )
    args = parser.parse_args()

    today = datetime.today().strftime("%Y-%m-%d")
    report = build_report(today)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(f"Report written → {args.output}")
    print(f"  {len(report.splitlines())} lines")


if __name__ == "__main__":
    main()
