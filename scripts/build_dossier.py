#!/usr/bin/env python3
"""Per-supplier dossier generator.

Reads normalized procurement tables and supplier clusters, then emits
one JSON + one HTML file per supplier cluster.

Usage:
    python scripts/build_dossier.py --org 6166
    python scripts/build_dossier.py --org 6166 --top 20
    python scripts/build_dossier.py --org 6166 --cluster-id cluster:abc123def456
    python scripts/build_dossier.py --org 6166 --format json
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


DEFAULT_INPUT_DIR = Path("data/normalized")
DEFAULT_OUTPUT_DIR = Path("reports/dossiers")
DEFAULT_TOP = 50


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def eur(value: Any) -> str:
    try:
        f = float(value)
        if math.isnan(f):
            return "—"
        return f"€{f:,.2f}"
    except (TypeError, ValueError):
        return "—"


def html_escape(text: Any) -> str:
    return str(text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def build_dossier_data(
    cluster: dict[str, Any],
    procurements: list[dict[str, Any]],
) -> dict[str, Any]:
    member_keys = set((cluster.get("member_keys") or "").split("|")) - {""}

    supplier_procs = [
        p for p in procurements
        if (p.get("supplier_key") or "") in member_keys
        or (cluster.get("supplier_tax_id") and p.get("supplier_tax_id") == cluster["supplier_tax_id"])
    ]

    amounts = [float(p["amount"]) for p in supplier_procs if p.get("amount") and p["amount"] not in ("", "nan")]
    total_amount = sum(amounts)
    avg_amount = total_amount / len(amounts) if amounts else 0.0
    max_amount = max(amounts) if amounts else 0.0

    years: dict[str, float] = {}
    for p in supplier_procs:
        year = str(p.get("year") or "")
        if year and p.get("amount") and p["amount"] not in ("", "nan"):
            years[year] = years.get(year, 0.0) + float(p["amount"])

    subject_types: dict[str, int] = {}
    for p in supplier_procs:
        dt = str(p.get("decision_type") or "unknown")
        subject_types[dt] = subject_types.get(dt, 0) + 1

    top_procurements = sorted(
        [p for p in supplier_procs if p.get("amount") and p["amount"] not in ("", "nan")],
        key=lambda p: float(p["amount"]),
        reverse=True,
    )[:20]

    return {
        "cluster_id": cluster.get("cluster_id"),
        "canonical_name": cluster.get("canonical_name"),
        "supplier_tax_id": cluster.get("supplier_tax_id"),
        "member_keys": list(member_keys),
        "decision_count": len(supplier_procs),
        "total_amount": total_amount,
        "avg_amount": avg_amount,
        "max_amount": max_amount,
        "first_seen": cluster.get("first_seen"),
        "last_seen": cluster.get("last_seen"),
        "amount_by_year": years,
        "decision_types": subject_types,
        "top_procurements": top_procurements,
    }


def render_html(dossier: dict[str, Any]) -> str:
    name = html_escape(dossier["canonical_name"] or dossier["cluster_id"])
    tax_id = html_escape(dossier["supplier_tax_id"] or "—")

    year_rows = "".join(
        f"<tr><td>{html_escape(y)}</td><td>{eur(a)}</td></tr>"
        for y, a in sorted(dossier["amount_by_year"].items())
    )

    type_rows = "".join(
        f"<tr><td>{html_escape(dt)}</td><td>{cnt}</td></tr>"
        for dt, cnt in sorted(dossier["decision_types"].items(), key=lambda x: -x[1])
    )

    proc_rows = "".join(
        f"<tr>"
        f"<td>{html_escape(p.get('issue_date',''))}</td>"
        f"<td><a href='{html_escape(p.get('url',''))}' target='_blank'>{html_escape(p.get('ada',''))}</a></td>"
        f"<td>{eur(p.get('amount'))}</td>"
        f"<td>{html_escape(str(p.get('subject',''))[:120])}</td>"
        f"<td>{html_escape(p.get('decision_type',''))}</td>"
        f"</tr>"
        for p in dossier["top_procurements"]
    )

    return f"""<!DOCTYPE html>
<html lang="el">
<head>
<meta charset="utf-8">
<title>Dossier: {name}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 32px; color: #222; }}
h1 {{ border-bottom: 2px solid #555; padding-bottom: 8px; }}
h2 {{ margin-top: 28px; color: #444; }}
.card {{ display: inline-block; padding: 14px 20px; margin: 6px; border: 1px solid #ddd; border-radius: 8px; min-width: 140px; }}
.card .label {{ font-size: 12px; color: #888; }}
.card .value {{ font-size: 22px; font-weight: bold; margin-top: 4px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 8px; }}
th, td {{ border: 1px solid #ddd; padding: 6px 10px; vertical-align: top; text-align: left; }}
th {{ background: #f5f5f5; }}
tr:hover {{ background: #fafafa; }}
a {{ color: #0066cc; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<h1>{name}</h1>
<p><strong>Tax ID:</strong> {tax_id} &nbsp; <strong>Cluster:</strong> {html_escape(dossier['cluster_id'])}</p>
<p><strong>Active:</strong> {html_escape(dossier['first_seen'] or '—')} → {html_escape(dossier['last_seen'] or '—')}</p>

<div>
  <div class="card"><div class="label">Procurements</div><div class="value">{dossier['decision_count']:,}</div></div>
  <div class="card"><div class="label">Total awarded</div><div class="value">{eur(dossier['total_amount'])}</div></div>
  <div class="card"><div class="label">Average</div><div class="value">{eur(dossier['avg_amount'])}</div></div>
  <div class="card"><div class="label">Largest</div><div class="value">{eur(dossier['max_amount'])}</div></div>
</div>

<h2>Amount by year</h2>
<table>
<tr><th>Year</th><th>Total</th></tr>
{year_rows or '<tr><td colspan="2">No data</td></tr>'}
</table>

<h2>Decision types</h2>
<table>
<tr><th>Type</th><th>Count</th></tr>
{type_rows or '<tr><td colspan="2">No data</td></tr>'}
</table>

<h2>Top procurements by value</h2>
<table>
<tr><th>Date</th><th>ADA</th><th>Amount</th><th>Subject</th><th>Type</th></tr>
{proc_rows or '<tr><td colspan="5">No data</td></tr>'}
</table>

</body>
</html>"""


def safe_filename(text: str) -> str:
    import re
    return re.sub(r"[^0-9A-Za-zΑ-Ωα-ωΆ-ώ._-]+", "_", text).strip("._")[:80]


def write_dossier(dossier: dict[str, Any], output_dir: Path, output_format: str) -> Path:
    name = dossier.get("canonical_name") or dossier.get("cluster_id") or "unknown"
    filename = safe_filename(name)
    output_dir.mkdir(parents=True, exist_ok=True)

    if output_format in ("both", "json"):
        json_path = output_dir / f"{filename}.json"
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(dossier, f, ensure_ascii=False, indent=2, default=str)

    if output_format in ("both", "html"):
        html_path = output_dir / f"{filename}.html"
        html_path.write_text(render_html(dossier), encoding="utf-8")

    return output_dir / filename


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate per-supplier dossiers from normalized procurement tables.")
    parser.add_argument("--org", required=True, help="Diavgeia organizationUid, e.g. 6166")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help=f"Top N suppliers by total amount (default: {DEFAULT_TOP})")
    parser.add_argument("--cluster-id", default=None, help="Generate dossier for a specific cluster ID only")
    parser.add_argument(
        "--format",
        choices=("json", "html", "both"),
        default="both",
        help="Output format (default: both)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base = args.input_dir / f"org={args.org}"

    procurements_path = base / "procurements.csv"
    if not procurements_path.exists():
        print(f"procurements.csv not found: {procurements_path}")
        return 1

    clusters_path = base / "supplier_clusters.csv"
    if not clusters_path.exists():
        print(f"supplier_clusters.csv not found: {clusters_path}")
        print("Run scripts/cluster_suppliers.py first.")
        return 1

    print("Loading data...")
    procurements = read_csv(procurements_path)
    clusters = read_csv(clusters_path)
    print(f"  {len(procurements)} procurements, {len(clusters)} clusters")

    if args.cluster_id:
        clusters = [c for c in clusters if c.get("cluster_id") == args.cluster_id]
        if not clusters:
            print(f"No cluster found with id={args.cluster_id}")
            return 1
    else:
        clusters = clusters[: args.top]

    output_dir = args.output_dir / f"org={args.org}"
    written = 0
    for cluster in clusters:
        dossier = build_dossier_data(cluster, procurements)
        write_dossier(dossier, output_dir, args.format)
        written += 1
        print(
            f"  {cluster.get('canonical_name') or cluster.get('cluster_id')}: "
            f"{dossier['decision_count']} procurements, {eur(dossier['total_amount'])}"
        )

    print(f"\nWrote {written} dossiers to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
