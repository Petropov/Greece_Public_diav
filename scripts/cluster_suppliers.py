#!/usr/bin/env python3
"""Supplier entity clustering — deduplicate and group supplier records.

Two-pass clustering:
  1. Tax-ID pass: suppliers sharing the same normalized tax_id are merged.
  2. Name pass:   remaining suppliers (no tax_id) with identical canonical
     names are merged.

Outputs supplier_clusters.csv with one row per canonical supplier entity.

Usage:
    python scripts/cluster_suppliers.py --org 6166
    python scripts/cluster_suppliers.py --org 6166 --input-dir data/normalized --output-dir data/normalized
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any


DEFAULT_INPUT_DIR = Path("data/normalized")
DEFAULT_OUTPUT_DIR = Path("data/normalized")

CLUSTER_COLUMNS = [
    "cluster_id",
    "canonical_name",
    "supplier_tax_id",
    "member_keys",
    "decision_count",
    "total_amount",
    "first_seen",
    "last_seen",
]


def canonical_text(value: Any) -> str:
    if value in (None, "", []):
        return ""
    if isinstance(value, float) and value != value:
        return ""
    text = unicodedata.normalize("NFD", str(value).lower())
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^0-9a-zα-ω]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_tax_id(value: Any) -> str | None:
    if not value or (isinstance(value, float) and value != value):
        return None
    compact = re.sub(r"[^0-9A-Za-z]", "", str(value)).upper()
    if not compact or compact == "EL":
        return None
    m = re.fullmatch(r"(?:EL)?(\d{9})", compact)
    if m:
        return m.group(1)
    seqs = re.findall(r"\d+", str(value))
    if len(seqs) == 1 and len(seqs[0]) == 9:
        return seqs[0]
    return None


def coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def read_suppliers_csv(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def write_clusters_csv(path: Path, clusters: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CLUSTER_COLUMNS)
        writer.writeheader()
        for cluster in clusters:
            writer.writerow({col: cluster.get(col, "") for col in CLUSTER_COLUMNS})


def cluster_id_from_key(key: str) -> str:
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"cluster:{digest}"


def merge_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge a list of supplier rows into one canonical cluster dict."""
    names = [r.get("supplier_name_normalized") or "" for r in rows if r.get("supplier_name_normalized")]
    tax_id = next((normalize_tax_id(r.get("supplier_tax_id")) for r in rows if normalize_tax_id(r.get("supplier_tax_id"))), None)
    canonical_name = max(names, key=len) if names else ""

    decision_count = sum(int(r.get("decision_count") or 0) for r in rows)
    total_amount = sum(coerce_float(r.get("total_amount")) for r in rows)

    dates = [r.get("first_seen") for r in rows if r.get("first_seen")]
    dates += [r.get("last_seen") for r in rows if r.get("last_seen")]
    first_seen = min(dates) if dates else ""
    last_seen = max(dates) if dates else ""

    member_keys = "|".join(sorted(set(r.get("supplier_key") or "" for r in rows if r.get("supplier_key"))))

    sort_key = tax_id or canonical_name or member_keys
    cid = cluster_id_from_key(sort_key)

    return {
        "cluster_id": cid,
        "canonical_name": canonical_name,
        "supplier_tax_id": tax_id or "",
        "member_keys": member_keys,
        "decision_count": decision_count,
        "total_amount": total_amount,
        "first_seen": first_seen,
        "last_seen": last_seen,
    }


def build_clusters(supplier_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tax_id_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    no_tax_id: list[dict[str, Any]] = []

    for row in supplier_rows:
        tid = normalize_tax_id(row.get("supplier_tax_id"))
        if tid:
            tax_id_groups[tid].append(row)
        else:
            no_tax_id.append(row)

    name_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ungrouped: list[dict[str, Any]] = []
    for row in no_tax_id:
        canon = canonical_text(row.get("supplier_name_normalized") or "")
        if canon:
            name_groups[canon].append(row)
        else:
            ungrouped.append(row)

    clusters = []

    for tid, rows in sorted(tax_id_groups.items()):
        clusters.append(merge_rows(rows))

    for canon, rows in sorted(name_groups.items()):
        clusters.append(merge_rows(rows))

    for row in ungrouped:
        clusters.append(merge_rows([row]))

    clusters.sort(key=lambda c: -coerce_float(c.get("total_amount")))
    return clusters


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cluster supplier records by tax ID then canonical name.")
    parser.add_argument("--org", required=True, help="Diavgeia organizationUid, e.g. 6166")
    parser.add_argument(
        "--input-dir", type=Path, default=DEFAULT_INPUT_DIR, help="Normalized tables directory (default: data/normalized)"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory (default: data/normalized)"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    suppliers_path = args.input_dir / f"org={args.org}" / "suppliers.csv"
    if not suppliers_path.exists():
        print(f"suppliers.csv not found: {suppliers_path}")
        return 1

    supplier_rows = read_suppliers_csv(suppliers_path)
    print(f"Loaded {len(supplier_rows)} supplier rows")

    clusters = build_clusters(supplier_rows)
    print(f"Built {len(clusters)} clusters")

    out_path = args.output_dir / f"org={args.org}" / "supplier_clusters.csv"
    write_clusters_csv(out_path, clusters)
    print(f"Wrote {out_path}")

    with_tax_id = sum(1 for c in clusters if c.get("supplier_tax_id"))
    multi_member = sum(1 for c in clusters if "|" in str(c.get("member_keys", "")))
    print(f"  with tax_id: {with_tax_id}, multi-member clusters: {multi_member}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
