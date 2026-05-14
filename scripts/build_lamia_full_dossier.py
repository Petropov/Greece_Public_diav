#!/usr/bin/env python3
"""Build a rich Markdown dossier from the Lamia ingestion outputs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.lamia_digest import canonical_text, format_amount, is_procurement_decision, normalize_decision  # noqa: E402

TREND_WARNING = (
    "Trend warning: pre-2026 months have weaker hydration and/or incomplete pagination, "
    "so comparisons with 2026 may reflect ingestion quality rather than real procurement behavior."
)
THEMES = {
    "digital / IT / software": ("software", "ψηφια", "πληροφορικ", "υπολογισ", "it", "δικτυ", "ιστοσελ"),
    "culture / festivals / events": ("πολιτισ", "φεστιβαλ", "εκδηλω", "event", "συναυλ"),
    "fleet / vehicles / spare parts": ("οχημ", "αυτοκινη", "ανταλλακτικ", "ελαστικ", "καυσιμ"),
    "energy / utilities": ("ενεργ", "ρευμα", "ηλεκτρ", "υδρευση", "φυσικο αεριο"),
    "food / welfare": ("τροφ", "συσσιτ", "προνοια", "κοινωνικ", "welfare"),
    "construction / maintenance": ("εργο", "συντηρη", "επισκευ", "κατασκευ", "οδοποι", "κτιρι"),
    "legal / consulting": ("νομικ", "συμβουλ", "μελετ", "consult"),
    "cleaning / environment / green spaces": ("καθαριοτ", "απορριμμα", "πρασιν", "περιβαλλον", "ανακυκλ"),
    "civil protection": ("πολιτικη προστασια", "πυροπροστα", "εκτακτ", "χιον"),
}


def read_table(path_base: Path) -> pd.DataFrame:
    csv_path = path_base.with_suffix(".csv")
    parquet_path = path_base.with_suffix(".parquet")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    return pd.DataFrame()


def md_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> str:
    if df.empty:
        return "_No rows available._\n"
    view = df[columns].head(limit) if limit else df[columns]
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in view.fillna("—").to_dict("records"):
        lines.append("| " + " | ".join(str(row[col]).replace("\n", " ").replace("|", "\\|") for col in columns) + " |")
    return "\n".join(lines) + "\n"


def classify_theme(row: dict[str, Any]) -> str:
    text = canonical_text(" ".join(str(row.get(k) or "") for k in ("subject", "decision_type", "supplier_name")))
    for theme, tokens in THEMES.items():
        if any(canonical_text(token) in text for token in tokens):
            return theme
    return "other"


def raw_cache_path(raw_root: Path, org: str, ada: str) -> Path:
    return raw_root / f"org={org}" / f"ada={ada}.json"


def json_snippet(raw_root: Path, org: str, ada: Any, max_chars: int = 1800) -> str:
    if pd.isna(ada) or not ada:
        return "_No ADA available._"
    path = raw_cache_path(raw_root, org, str(ada))
    if not path.exists():
        return f"_Raw JSON cache missing for {ada}._"
    payload = json.loads(path.read_text(encoding="utf-8"))
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n..."
    return f"```json\n{text}\n```"


def prepare_procurement(index_df: pd.DataFrame, hydrated_df: pd.DataFrame) -> pd.DataFrame:
    if index_df.empty:
        return pd.DataFrame()
    hyd = hydrated_df.set_index("ada") if not hydrated_df.empty and "ada" in hydrated_df else pd.DataFrame()
    rows = []
    for row in index_df.to_dict("records"):
        merged = dict(row)
        ada = row.get("ada")
        if not hyd.empty and ada in hyd.index:
            detail_row = hyd.loc[ada]
            if isinstance(detail_row, pd.DataFrame):
                detail_row = detail_row.iloc[0]
            for key, value in detail_row.to_dict().items():
                if pd.notna(value):
                    merged[key] = value
        if is_procurement_decision(normalize_decision(merged)):
            rows.append(merged)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["theme"] = [classify_theme(row) for row in df.to_dict("records")]
    return df


def build_dossier(org: str, raw_root: Path, index_root: Path, quality_root: Path, normalized_root: Path) -> str:
    index_df = read_table(index_root / f"org={org}" / "decision_index")
    coverage_df = pd.read_csv(quality_root / f"org={org}" / "monthly_coverage.csv") if (quality_root / f"org={org}" / "monthly_coverage.csv").exists() else pd.DataFrame()
    failures_df = pd.read_csv(quality_root / f"org={org}" / "failed_hydrations.csv") if (quality_root / f"org={org}" / "failed_hydrations.csv").exists() else pd.DataFrame()
    hydrated_df = read_table(normalized_root / f"org={org}" / "hydrated_decisions")
    procurement_df = prepare_procurement(index_df, hydrated_df)

    known_amount = float(pd.to_numeric(hydrated_df.get("amount", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not hydrated_df.empty else 0.0
    top_amounts = hydrated_df.copy() if not hydrated_df.empty else pd.DataFrame()
    if not top_amounts.empty:
        top_amounts["amount_num"] = pd.to_numeric(top_amounts["amount"], errors="coerce")
        top_amounts = top_amounts.dropna(subset=["amount_num"]).sort_values("amount_num", ascending=False)
        top_amounts["amount"] = top_amounts["amount_num"].map(format_amount)

    direct = hydrated_df[hydrated_df.get("decision_type", pd.Series(dtype=str)).astype(str).str.startswith("Δ.1", na=False)].copy() if not hydrated_df.empty else pd.DataFrame()
    direct_spend = float(pd.to_numeric(direct.get("amount", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not direct.empty else 0.0

    supplier_amount = pd.DataFrame()
    supplier_count = pd.DataFrame()
    high_freq_missing = pd.DataFrame()
    if not hydrated_df.empty and "supplier_name" in hydrated_df:
        tmp = hydrated_df.copy()
        tmp["amount_num"] = pd.to_numeric(tmp.get("amount"), errors="coerce")
        supplier_amount = tmp.dropna(subset=["supplier_name"]).groupby("supplier_name", as_index=False).agg(known_amount=("amount_num", "sum"), decisions=("ada", "count")).sort_values("known_amount", ascending=False)
        supplier_amount["known_amount"] = supplier_amount["known_amount"].map(format_amount)
        supplier_count = tmp.dropna(subset=["supplier_name"]).groupby("supplier_name", as_index=False).agg(decisions=("ada", "count"), known_amount=("amount_num", "sum")).sort_values("decisions", ascending=False)
        supplier_count["known_amount"] = supplier_count["known_amount"].map(format_amount)
        missing = tmp[tmp["amount_num"].isna()].dropna(subset=["supplier_name"])
        high_freq_missing = missing.groupby("supplier_name", as_index=False).agg(missing_amount_rows=("ada", "count")).sort_values("missing_amount_rows", ascending=False)

    theme_summary = pd.DataFrame()
    if not procurement_df.empty:
        theme_summary = procurement_df.groupby("theme", as_index=False).agg(rows=("ada", "count")).sort_values("rows", ascending=False)

    capped_months = coverage_df[coverage_df.get("exact_500_flag", False) == True] if not coverage_df.empty else pd.DataFrame()
    low_conf = coverage_df[coverage_df.get("confidence_rating", "") != "green"] if not coverage_df.empty else pd.DataFrame()

    repeated_supplier_ada = None
    if not supplier_count.empty:
        supplier = supplier_count.iloc[0]["supplier_name"]
        match = hydrated_df[hydrated_df["supplier_name"] == supplier]
        if not match.empty:
            repeated_supplier_ada = match.iloc[0].get("ada")

    missing_nested = hydrated_df[hydrated_df.get("amount_source", pd.Series(dtype=str)).astype(str).str.contains("extraFieldValues|raw:", na=False)].head(1) if not hydrated_df.empty else pd.DataFrame()
    subject_only = hydrated_df[hydrated_df.get("amount_source", pd.Series(dtype=str)).astype(str).str.contains("subject", na=False)].head(1) if not hydrated_df.empty else pd.DataFrame()
    largest_ada = top_amounts.iloc[0].get("ada") if not top_amounts.empty else None
    largest_payment = hydrated_df[hydrated_df.get("decision_type", pd.Series(dtype=str)).astype(str).str.startswith("Β.2", na=False)].copy() if not hydrated_df.empty else pd.DataFrame()
    if not largest_payment.empty:
        largest_payment["amount_num"] = pd.to_numeric(largest_payment["amount"], errors="coerce")
        largest_payment = largest_payment.sort_values("amount_num", ascending=False)

    lines = [
        f"# Lamia Municipality Diavgeia Full Dossier (org {org})",
        "",
        "## Dataset Coverage & Reliability",
        TREND_WARNING,
        "",
        md_table(coverage_df, ["year", "month", "indexed_decisions", "hydrated_decisions", "hydration_pct", "procurement_rows", "rows_with_amount", "amount_extraction_pct", "rows_with_supplier", "supplier_extraction_pct", "exact_500_flag", "pagination_complete", "failed_hydrations", "confidence_rating"] if not coverage_df.empty else []),
        "## Executive Summary",
        f"* Total indexed decisions: {len(index_df):,}",
        f"* Hydrated decisions: {len(hydrated_df):,}",
        f"* Procurement-like rows: {len(procurement_df):,}",
        f"* Known spend in hydrated rows: {format_amount(known_amount)} EUR",
        f"* Major caveat: {TREND_WARNING}",
        "* Missing amount and supplier fields are data-quality findings, not proof that no supplier or spend existed.",
        "",
        "## Top Financial Items",
        md_table(top_amounts, ["ada", "issue_date", "decision_type", "subject", "supplier_name", "amount", "amount_source", "url"], 25),
        "## Direct Assignments",
        f"* Direct assignment rows hydrated: {len(direct):,}",
        f"* Known direct-assignment spend: {format_amount(direct_spend)} EUR",
        "",
        md_table(direct.sort_values("amount", ascending=False) if not direct.empty and "amount" in direct else direct, ["ada", "issue_date", "subject", "supplier_name", "amount", "amount_source", "url"] if not direct.empty else [], 25),
        "## Supplier Intelligence",
        "### Top suppliers by known amount",
        md_table(supplier_amount, ["supplier_name", "known_amount", "decisions"], 20),
        "### Top suppliers by count",
        md_table(supplier_count, ["supplier_name", "decisions", "known_amount"], 20),
        "### High-frequency suppliers with missing amounts",
        md_table(high_freq_missing, ["supplier_name", "missing_amount_rows"], 20),
        "## Theme Analysis",
        md_table(theme_summary, ["theme", "rows"] if not theme_summary.empty else []),
        "## Data Quality Issues",
        f"* Capped months: {', '.join(f'{int(r.year):04d}-{int(r.month):02d}' for r in capped_months.itertuples()) if not capped_months.empty else 'none flagged in monthly audit'}.",
        f"* Low-confidence months: {', '.join(f'{int(r.year):04d}-{int(r.month):02d}:{r.confidence_rating}' for r in low_conf.itertuples()) if not low_conf.empty else 'none'}.",
        f"* Failed hydrations tracked: {len(failures_df):,} rows in failed_hydrations.csv.",
        "* Weak pre-2026 hydration or incomplete pagination must be treated as an ingestion limitation; the dossier avoids year-over-year trend claims.",
        "* Examples below show cases where values can live in nested raw JSON or only in subject text, which is why provenance fields are retained.",
        "",
        "## Evidence Appendix",
        "### Largest tender / financial item raw JSON",
        json_snippet(raw_root, org, largest_ada),
        "### Largest payment raw JSON",
        json_snippet(raw_root, org, largest_payment.iloc[0].get("ada") if not largest_payment.empty else None),
        "### Repeated supplier raw JSON",
        json_snippet(raw_root, org, repeated_supplier_ada),
        "### Missing amount recovered from nested JSON",
        json_snippet(raw_root, org, missing_nested.iloc[0].get("ada") if not missing_nested.empty else None),
        "### Amount existed only inside subject text",
        json_snippet(raw_root, org, subject_only.iloc[0].get("ada") if not subject_only.empty else None),
        "### Capped / low-confidence month evidence",
        md_table(low_conf.head(10), ["year", "month", "indexed_decisions", "hydrated_decisions", "exact_500_flag", "pagination_complete", "confidence_rating"] if not low_conf.empty else []),
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Lamia full Markdown dossier")
    parser.add_argument("--org", default="6166")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw/diavgeia"))
    parser.add_argument("--index-root", type=Path, default=Path("data/index"))
    parser.add_argument("--quality-root", type=Path, default=Path("data/quality"))
    parser.add_argument("--normalized-root", type=Path, default=Path("data/normalized"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(build_dossier(args.org, args.raw_root, args.index_root, args.quality_root, args.normalized_root), encoding="utf-8")
    print(f"Wrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
