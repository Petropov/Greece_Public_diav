#!/usr/bin/env python3
"""Selective ADA hydration — search-wide scan, narrow deep-fetch.

Reads all cached search_export.json files for an org and scores each
decision for hydration value.  Only fetches the detail endpoint for
decisions that exceed the minimum score threshold, skipping:
  - decisions already hydrated (decisions/ADA.json exists)
  - payroll/admin noise (low procurement signal)
  - decisions where the search export already has amount + supplier

Usage:
    python scripts/hydrate_narrow.py --org 6166
    python scripts/hydrate_narrow.py --org 6166 --months 2024-01:2024-12
    python scripts/hydrate_narrow.py --org 6166 --min-score 2 --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

import requests

DEFAULT_RAW_ROOT = Path("data/raw/diavgeia")
DETAIL_URL_TEMPLATE = "https://diavgeia.gov.gr/opendata/decisions/{ada}"
DEFAULT_MIN_SCORE = 2
DEFAULT_REQUEST_DELAY = 0.3
MAX_RETRIES = 3

PROCUREMENT_TYPE_IDS = {
    "Δ.1",
    "Δ.2.1",
    "Δ.2.2",
    "Δ.2.3",
    "Β.1.1",
    "Β.2.1",
    "Β.2.2",
    "Β.1.3",
}

PROCUREMENT_SUBJECT_TOKENS = (
    "συμβαση",
    "αναθεση",
    "προμηθεια",
    "δαπανη",
    "προϋπολογισμ",
    "αναδοχ",
    "εκτελεση",
    "διακηρυξ",
    "διαγωνισμ",
    "υπηρεσια",
    "εργο",
    "μελετη",
    "εξοδα",
    "καυσιμ",
    "υλικ",
)

SKIP_TOKENS = (
    "συμβαση εργασιας ιδιωτικου δικαιου",
    "αποζημιωση υπερωρ",
    "αποζημιωση νυχτ",
    "μισθοδοσια",
    "κυρωση μισθοδοτ",
    "εγκριση αποδοχων",
    "εγκριση μισθοδοσιας",
    "προνοιακ επιδομα",
    "προκηρυξη πληρωσης θεσεων",
    "πινακα επιτυχοντων",
    "διοριστεων",
    "ορκωμοσια",
    "χορηγηση αδειας",
    "αδεια απουσιας",
    "μετακινηση υπαλληλ",
    "αποσπαση υπαλληλ",
    "αναθεση καθηκοντων",
    "τοποθετηση υπαλληλ",
    "εκπαιδευτικη αδεια",
    "συνδικαλιστικη αδεια",
    "αναρρωτικη αδεια",
    "παραιτηση υπαλληλ",
    "αυτοδικαιη λυση",
    "ακυρωση",
    "ανακληση",
    "απορριψη",
    "μη εγκριση",
)


def canonical_text(value: Any) -> str:
    if value in (None, "", []):
        return ""
    if isinstance(value, float) and value != value:
        return ""
    text = unicodedata.normalize("NFD", str(value).lower())
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^0-9a-zα-ω]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def safe_ada_filename(ada: str) -> str:
    return re.sub(r"[^0-9A-Za-zΑ-Ωα-ωΆ-ώ._-]+", "_", str(ada)).strip("._") or "unknown"


def month_dir(raw_root: Path, org: str, year: int, month: int) -> Path:
    return raw_root / f"organization_uid={org}" / f"year={year:04d}" / f"month={month:02d}"


def search_export_path(raw_root: Path, org: str, year: int, month: int) -> Path:
    return month_dir(raw_root, org, year, month) / "search_export.json"


def decision_cache_path(raw_root: Path, org: str, year: int, month: int, ada: str) -> Path:
    return month_dir(raw_root, org, year, month) / "decisions" / f"{safe_ada_filename(ada)}.json"


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("decisionResultList", "decisions", "diavgeia_decisions"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    dr = payload.get("decisionResults") or payload.get("decisionresults")
    if isinstance(dr, dict):
        rows = dr.get("decision") or dr.get("decisions") or []
        if isinstance(rows, dict):
            return [rows]
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def has_amount(row: dict[str, Any]) -> bool:
    for key in ("amount", "paymentAmount", "expenseAmount", "netAmount", "totalAmount", "amountWithVAT", "budgetAmount"):
        val = row.get(key)
        if val not in (None, "", 0, 0.0):
            return True
    return False


def has_supplier(row: dict[str, Any]) -> bool:
    for key in ("supplierName", "supplier_name", "contractorName", "contractorTitle", "beneficiaryName"):
        if row.get(key):
            return True
    return False


def score_decision(row: dict[str, Any]) -> int:
    """Return a hydration priority score.  Higher = more worth fetching detail."""
    raw_type = str(row.get("decisionTypeUid") or row.get("decisionTypeId") or row.get("type") or "")
    subject = canonical_text(row.get("subject") or "")
    combined = canonical_text(f"{raw_type} {subject}")

    padded = f" {combined} "
    if any(f" {token}" in padded or padded.startswith(token) for token in SKIP_TOKENS):
        return -10

    score = 0

    if raw_type in PROCUREMENT_TYPE_IDS:
        score += 3

    subject_hits = sum(1 for token in PROCUREMENT_SUBJECT_TOKENS if token in combined)
    score += min(subject_hits, 3)

    if has_amount(row) and has_supplier(row):
        score -= 2

    return score


def fetch_detail(ada: str, timeout: int = 30) -> dict[str, Any]:
    url = DETAIL_URL_TEMPLATE.format(ada=ada)
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    raise last_exc  # type: ignore[misc]


def parse_month_range(value: str) -> list[tuple[int, int]]:
    """Parse 'YYYY-MM' or 'YYYY-MM:YYYY-MM' into list of (year, month) tuples."""
    if ":" in value:
        start_str, end_str = value.split(":", 1)
    else:
        start_str = end_str = value
    start = date.fromisoformat(start_str.strip() + "-01")
    end = date.fromisoformat(end_str.strip() + "-01")
    months = []
    cur = start
    while cur <= end:
        months.append((cur.year, cur.month))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return months


def find_all_months(raw_root: Path, org: str) -> list[tuple[int, int]]:
    org_dir = raw_root / f"organization_uid={org}"
    months = []
    for search_path in sorted(org_dir.glob("year=*/month=*/search_export.json")):
        parts = search_path.parts
        year_part = next((p for p in parts if p.startswith("year=")), None)
        month_part = next((p for p in parts if p.startswith("month=")), None)
        if year_part and month_part:
            months.append((int(year_part.split("=")[1]), int(month_part.split("=")[1])))
    return months


def hydrate_month(
    raw_root: Path,
    org: str,
    year: int,
    month: int,
    *,
    min_score: int,
    dry_run: bool,
    request_delay: float,
    verbose: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {"scanned": 0, "already_hydrated": 0, "below_threshold": 0, "fetched": 0, "errors": 0}

    search_path = search_export_path(raw_root, org, year, month)
    if not search_path.exists():
        return stats

    rows = extract_rows(read_json(search_path))
    stats["scanned"] = len(rows)

    for row in rows:
        ada = str(row.get("ada") or row.get("ADA") or "").strip()
        if not ada:
            continue

        cache_path = decision_cache_path(raw_root, org, year, month, ada)
        if cache_path.exists():
            stats["already_hydrated"] += 1
            continue

        score = score_decision(row)
        if score < min_score:
            stats["below_threshold"] += 1
            if verbose:
                print(f"  SKIP score={score:+d} {ada}: {str(row.get('subject') or '')[:60]}")
            continue

        if dry_run:
            print(f"  DRY-RUN would fetch score={score:+d} {ada}: {str(row.get('subject') or '')[:60]}")
            stats["fetched"] += 1
            continue

        try:
            detail = fetch_detail(ada)
            write_json(cache_path, detail)
            stats["fetched"] += 1
            if verbose:
                print(f"  FETCH score={score:+d} {ada}: {str(row.get('subject') or '')[:60]}")
            time.sleep(request_delay)
        except Exception as exc:
            stats["errors"] += 1
            print(f"  ERROR {ada}: {exc}")

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Selective ADA hydration — fetch detail JSONs for high-value procurement decisions only."
    )
    parser.add_argument("--org", required=True, help="Diavgeia organizationUid, e.g. 6166")
    parser.add_argument(
        "--raw-root", type=Path, default=DEFAULT_RAW_ROOT, help="Raw cache root (default: data/raw/diavgeia)"
    )
    parser.add_argument(
        "--months",
        default=None,
        help="Month range to hydrate, e.g. 2024-01 or 2024-01:2024-12 (default: all cached months)",
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum hydration score to fetch detail (default: {DEFAULT_MIN_SCORE})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be fetched without hitting the API")
    parser.add_argument("--verbose", action="store_true", help="Print per-decision actions")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help=f"Seconds between API requests (default: {DEFAULT_REQUEST_DELAY})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.months:
        months = parse_month_range(args.months)
    else:
        months = find_all_months(args.raw_root, args.org)

    if not months:
        print(f"No cached months found for org={args.org} in {args.raw_root}")
        return 1

    total: dict[str, int] = {"scanned": 0, "already_hydrated": 0, "below_threshold": 0, "fetched": 0, "errors": 0}

    for year, month in months:
        label = f"{year:04d}-{month:02d}"
        stats = hydrate_month(
            args.raw_root,
            args.org,
            year,
            month,
            min_score=args.min_score,
            dry_run=args.dry_run,
            request_delay=args.delay,
            verbose=args.verbose,
        )
        action = "DRY-RUN" if args.dry_run else "hydrated"
        print(
            f"{label}: scanned={stats['scanned']} already={stats['already_hydrated']} "
            f"skip={stats['below_threshold']} {action}={stats['fetched']} errors={stats['errors']}"
        )
        for key in total:
            total[key] += stats[key]

    print(
        f"\nTotal: scanned={total['scanned']} already={total['already_hydrated']} "
        f"skip={total['below_threshold']} fetched={total['fetched']} errors={total['errors']}"
    )
    return 0 if total["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
