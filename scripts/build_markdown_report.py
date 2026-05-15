#!/usr/bin/env python3
"""Generate a comprehensive Markdown intelligence report from normalized tables.

Reads all four normalized CSVs for an org and writes a single .md file
covering: executive summary, yearly/monthly spend, top procurements,
top suppliers, decision-type breakdown, and data-coverage notes.

Usage:
    python scripts/build_markdown_report.py --org 6166
    python scripts/build_markdown_report.py --org 6166 --input-dir data/normalized --output reports/lamia_intelligence.md
"""
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


DEFAULT_INPUT_DIR = Path("data/normalized")
DEFAULT_OUTPUT_DIR = Path("reports")


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def eur(value: Any) -> str:
    try:
        f = float(value)
        if math.isnan(f) or f == 0:
            return "—"
        if f >= 1_000_000:
            return f"€{f/1_000_000:.2f}M"
        if f >= 1_000:
            return f"€{f:,.0f}"
        return f"€{f:.2f}"
    except (TypeError, ValueError):
        return "—"


def pct(num: int, denom: int) -> str:
    if denom == 0:
        return "—"
    return f"{num/denom:.0%}"


def safe_float(value: Any) -> float:
    try:
        f = float(value)
        return 0.0 if math.isnan(f) else f
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def md_table(headers: list[str], rows: list[list[str]]) -> str:
    col_widths = [max(len(h), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(headers)]
    sep = "| " + " | ".join("-" * w for w in col_widths) + " |"
    header = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    data_rows = [
        "| " + " | ".join(str(r[i]).ljust(col_widths[i]) for i in range(len(headers))) + " |"
        for r in rows
    ]
    return "\n".join([header, sep] + data_rows)


def build_report(
    org: str,
    decisions: list[dict],
    procurements: list[dict],
    suppliers: list[dict],
    monthly_summary: list[dict],
    contracts: list[dict] | None = None,
    gemi: list[dict] | None = None,
) -> str:
    sections: list[str] = []

    # ── 1. Executive summary ──────────────────────────────────────────────────
    total_decisions = sum(safe_int(r["decision_count"]) for r in monthly_summary)
    total_amount = sum(safe_float(r["amount_total"]) for r in monthly_summary)
    years = sorted(set(r["year"] for r in monthly_summary))
    year_range = f"{years[0]}–{years[-1]}" if years else "—"
    total_suppliers = len(set(r["supplier_key"] for r in suppliers if r.get("supplier_key")))
    with_amount = [p for p in procurements if p.get("amount") and p["amount"] not in ("", "nan")]
    with_tax = [p for p in procurements if p.get("supplier_tax_id")]

    # Use deduplicated contract spend if available
    clean_amount: float | None = None
    if contracts:
        clean_amount = sum(safe_float(c.get("amount", 0)) for c in contracts if safe_float(c.get("amount", 0)) > 0)

    sections.append(f"# Δήμος Λαμιέων (org={org}) — Procurement Intelligence Report\n")
    spend_note = (
        f"Clean spend (deduplicated): **{eur(clean_amount)}** · Raw (with multi-stage): **{eur(total_amount)}**"
        if clean_amount is not None
        else f"Total spend tracked: **{eur(total_amount)}**"
    )
    sections.append(
        f"> Data range: **{year_range}** · "
        f"Decisions: **{total_decisions:,}** · "
        f"Procurements: **{len(procurements):,}** · "
        f"Known suppliers: **{total_suppliers:,}** · "
        f"{spend_note}\n"
    )

    # ── 2. Yearly spend ───────────────────────────────────────────────────────
    yearly: dict[str, dict[str, float | int]] = defaultdict(lambda: {"amount": 0.0, "decisions": 0, "suppliers": 0})
    for r in monthly_summary:
        y = r["year"]
        yearly[y]["amount"] += safe_float(r["amount_total"])
        yearly[y]["decisions"] += safe_int(r["decision_count"])
        yearly[y]["suppliers"] += safe_int(r["supplier_count"])

    sections.append("## Spend by Year\n")
    rows = [
        [y, eur(d["amount"]), f"{d['decisions']:,}", str(d["suppliers"])]
        for y, d in sorted(yearly.items())
    ]
    sections.append(md_table(["Year", "Total spend", "Decisions", "Supplier count"], rows))
    sections.append("")

    # ── 3. Monthly detail ─────────────────────────────────────────────────────
    sections.append("## Monthly Breakdown\n")
    ms_sorted = sorted(monthly_summary, key=lambda r: (r["year"], r["month"].zfill(2)))
    rows = []
    for r in ms_sorted:
        month_label = f"{r['year']}-{r['month'].zfill(2)}"
        amount = safe_float(r["amount_total"])
        decisions = safe_int(r["decision_count"])
        known = safe_int(r["amount_known_count"])
        enriched = safe_int(r["detail_enriched_decision_count"])
        rows.append([
            month_label,
            eur(amount) if amount > 0 else "—",
            str(decisions),
            pct(known, decisions),
            str(enriched),
        ])
    sections.append(md_table(
        ["Month", "Spend", "Decisions", "Amount coverage", "Detail-enriched"],
        rows,
    ))
    sections.append("")

    # ── 4. Top contracts / procurements by value ─────────────────────────────
    if contracts:
        top_source = sorted(
            [c for c in contracts if safe_float(c.get("amount", 0)) > 0],
            key=lambda c: -safe_float(c["amount"]),
        )[:30]
        section_title = "## Top 30 Contracts by Value (deduplicated)\n"
        note = "_Amounts deduplicated — each contract counted once across all lifecycle stages._\n"
        row_builder = lambda c: [
            (c.get("issue_date") or "—")[:10],
            c.get("ada") or "—",
            eur(c.get("amount")),
            (c.get("decision_type") or "—")[:30],
            str(c.get("subject") or "").replace("\n", " ")[:80],
        ]
        headers = ["Date", "ADA", "Amount", "Type", "Subject"]
    else:
        top_source = sorted(with_amount, key=lambda r: -safe_float(r["amount"]))[:30]
        section_title = "## Top 30 Procurements by Value\n"
        note = "_Run `link_procurement_lifecycle.py` for deduplicated contract view._\n"
        row_builder = lambda p: [
            p.get("issue_date") or "—",
            p.get("ada") or "—",
            eur(p.get("amount")),
            p.get("decision_type") or "—",
            str(p.get("subject") or "").replace("\n", " ")[:80],
        ]
        headers = ["Date", "ADA", "Amount", "Type", "Subject"]

    sections.append(section_title)
    sections.append(note)
    rows = [row_builder(item) for item in top_source]
    if rows:
        sections.append(md_table(headers, rows))
    else:
        sections.append("_No procurements with structured amounts found._")
    sections.append("")

    # ── 5. Top suppliers by spend ─────────────────────────────────────────────
    sections.append("## Top Suppliers by Total Spend\n")
    top_suppliers = sorted(
        [s for s in suppliers if safe_float(s.get("total_amount")) > 0],
        key=lambda s: -safe_float(s["total_amount"]),
    )[:30]
    rows = []
    for s in top_suppliers:
        name = str(s.get("supplier_name_normalized") or "—")[:40]
        tax_id = s.get("supplier_tax_id") or "—"
        rows.append([
            tax_id,
            name,
            eur(s.get("total_amount")),
            str(safe_int(s.get("decision_count"))),
            s.get("first_seen") or "—",
            s.get("last_seen") or "—",
        ])
    if rows:
        sections.append(md_table(
            ["Tax ID", "Name", "Total spend", "Decisions", "First seen", "Last seen"],
            rows,
        ))
    else:
        sections.append("_No suppliers with known spend._")
    sections.append("")

    # ── 6. Repeat suppliers (≥3 decisions) ───────────────────────────────────
    repeat = [s for s in suppliers if safe_int(s.get("decision_count")) >= 3]
    repeat = sorted(repeat, key=lambda s: -safe_int(s["decision_count"]))[:20]
    if repeat:
        sections.append("## Repeat Suppliers (≥ 3 decisions)\n")
        rows = [
            [
                s.get("supplier_tax_id") or "—",
                str(s.get("supplier_name_normalized") or "—")[:40],
                str(safe_int(s.get("decision_count"))),
                eur(s.get("total_amount")),
            ]
            for s in repeat
        ]
        sections.append(md_table(["Tax ID", "Name", "Decisions", "Total spend"], rows))
        sections.append("")

    # ── 7. Decision-type breakdown ────────────────────────────────────────────
    sections.append("## Decision Types in Procurement Table\n")
    dtype_counts: dict[str, int] = defaultdict(int)
    dtype_amounts: dict[str, float] = defaultdict(float)
    for p in procurements:
        dt = p.get("decision_type") or "unknown"
        dtype_counts[dt] += 1
        dtype_amounts[dt] += safe_float(p.get("amount"))
    dtype_rows = sorted(dtype_counts.items(), key=lambda x: -x[1])
    rows = [
        [dt[:60], f"{cnt:,}", eur(dtype_amounts[dt])]
        for dt, cnt in dtype_rows
    ]
    sections.append(md_table(["Decision type", "Count", "Spend"], rows))
    sections.append("")

    # ── 8. Procurement stage breakdown ───────────────────────────────────────
    if any(p.get("procurement_stage") for p in procurements):
        sections.append("## Procurement Stages\n")
        stage_counts: dict[str, int] = defaultdict(int)
        for p in procurements:
            stage_counts[p.get("procurement_stage") or "unknown"] += 1
        rows = [[s, f"{c:,}"] for s, c in sorted(stage_counts.items(), key=lambda x: -x[1])]
        sections.append(md_table(["Stage", "Count"], rows))
        sections.append("")

    # ── 9. ΓΕΜΗ transparency flags ────────────────────────────────────────────
    if gemi:
        flagged = {
            "flag_low_capital": [r for r in gemi if r.get("flag_low_capital") == "1"],
            "flag_recently_registered": [r for r in gemi if r.get("flag_recently_registered") == "1"],
            "flag_inactive": [r for r in gemi if r.get("flag_inactive") == "1"],
            "flag_no_gemi_record": [r for r in gemi if r.get("flag_no_gemi_record") == "1"],
        }
        total_flagged = len(set(
            r["supplier_tax_id"] for flags in flagged.values() for r in flags
        ))
        if total_flagged > 0:
            sections.append("## ΓΕΜΗ Transparency Flags\n")
            sections.append(
                f"_ΓΕΜΗ enrichment covers {len(gemi)} suppliers. "
                f"{total_flagged} have at least one flag._\n"
            )
            flag_labels = {
                "flag_low_capital": "Low share capital (< €10k) with large contract (> €100k)",
                "flag_recently_registered": "Company registered ≤ 12 months before first contract",
                "flag_inactive": "Company status inactive at time of enrichment",
                "flag_no_gemi_record": "No ΓΕΜΗ record found for this tax ID",
            }
            for flag, label in flag_labels.items():
                flagged_rows = flagged[flag]
                if not flagged_rows:
                    continue
                sections.append(f"### ⚑ {label} ({len(flagged_rows)} supplier(s))\n")
                sup_by_tax = {s["supplier_tax_id"]: s for s in suppliers}
                table_rows = []
                for r in flagged_rows:
                    tax = r.get("supplier_tax_id") or "—"
                    name = r.get("legal_name") or (sup_by_tax.get(tax, {}).get("supplier_name_normalized") or "—")
                    capital = eur(r.get("share_capital")) if r.get("share_capital") else "—"
                    reg_date = (r.get("registration_date") or "—")[:10]
                    spend = eur(sup_by_tax.get(tax, {}).get("total_amount"))
                    table_rows.append([tax, name[:40], capital, reg_date, spend])
                sections.append(md_table(
                    ["Tax ID", "Name", "Capital", "Registered", "Total spend"],
                    table_rows,
                ))
                sections.append("")

    # ── 10. Data coverage notes ───────────────────────────────────────────────
    sections.append("## Data Coverage Notes\n")
    amount_pct = len(with_amount) / len(procurements) * 100 if procurements else 0
    tax_pct = len(with_tax) / len(procurements) * 100 if procurements else 0
    enriched_total = sum(safe_int(r["detail_enriched_decision_count"]) for r in monthly_summary)
    search_only_total = sum(safe_int(r["search_only_decision_count"]) for r in monthly_summary)

    clean_spend_row = (
        f"| Clean spend (contracts deduplicated) | {eur(clean_amount)} |\n"
        if clean_amount is not None else ""
    )
    gemi_row = (
        f"| Suppliers enriched via ΓΕΜΗ | {len(gemi):,} |\n"
        if gemi else ""
    )

    sections.append(
        f"| Metric | Value |\n"
        f"|--------|-------|\n"
        f"| Total decisions | {total_decisions:,} |\n"
        f"| Detail-enriched decisions | {enriched_total:,} ({pct(enriched_total, total_decisions)}) |\n"
        f"| Search-only decisions | {search_only_total:,} ({pct(search_only_total, total_decisions)}) |\n"
        f"| Procurements with structured amount | {len(with_amount):,} ({amount_pct:.1f}%) |\n"
        f"| Procurements with supplier tax ID | {len(with_tax):,} ({tax_pct:.1f}%) |\n"
        f"| Unique supplier entities | {total_suppliers:,} |\n"
        f"| Total tracked spend (raw, may double-count) | {eur(total_amount)} |\n"
        + clean_spend_row
        + gemi_row
    )
    sections.append(
        "\n> **Note:** Low amount/supplier coverage in years 2018–2024 reflects search-only cache "
        "(no per-ADA detail enrichment). Run `scripts/hydrate_narrow.py` to selectively enrich "
        "high-value procurement decisions and improve coverage.\n"
    )

    return "\n".join(sections)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a comprehensive Markdown intelligence report.")
    parser.add_argument("--org", required=True, help="Diavgeia organizationUid, e.g. 6166")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", type=Path, default=None, help="Output .md path (default: reports/intelligence_org_<ORG>.md)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base = args.input_dir / f"org={args.org}"

    for name in ("decisions.csv", "procurements.csv", "suppliers.csv", "monthly_summary.csv"):
        if not (base / name).exists():
            print(f"Missing: {base / name}")
            return 1

    print("Loading tables...")
    decisions = read_csv(base / "decisions.csv")
    procurements = read_csv(base / "procurements.csv")
    suppliers = read_csv(base / "suppliers.csv")
    monthly_summary = read_csv(base / "monthly_summary.csv")
    print(f"  decisions={len(decisions)}, procurements={len(procurements)}, "
          f"suppliers={len(suppliers)}, months={len(monthly_summary)}")

    # Optional enrichment tables
    contracts: list[dict] | None = None
    gemi: list[dict] | None = None
    if (base / "contracts.csv").exists():
        contracts = read_csv(base / "contracts.csv")
        print(f"  contracts={len(contracts)} (deduplicated lifecycle view)")
    if (base / "gemi_enrichment.csv").exists():
        gemi = read_csv(base / "gemi_enrichment.csv")
        print(f"  gemi_enrichment={len(gemi)} supplier records")

    report = build_report(args.org, decisions, procurements, suppliers, monthly_summary,
                          contracts=contracts, gemi=gemi)

    out = args.output or (DEFAULT_OUTPUT_DIR / f"intelligence_org_{args.org}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")
    print(f"Wrote {out} ({len(report):,} chars)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
