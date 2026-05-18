#!/usr/bin/env python3
"""Targeted hydration for specific Diavgeia decision types.

Reads all cached search_export.json files for one or more orgs,
filters to decisions whose decisionTypeUid is in the target set,
and fetches their detail JSONs (skipping already-cached ones).

After hydration, re-runs normalize + lifecycle for each org so that
the updated amounts appear in decisions.csv / contracts.csv.

Usage:
    python scripts/hydrate_by_type.py --orgs 6166 6298 6154 6272
    python scripts/hydrate_by_type.py --orgs 6166 --type-uids Δ.1
    python scripts/hydrate_by_type.py --orgs 6166 6298 --dry-run
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
DEFAULT_RAW_ROOT = REPO_ROOT / "data" / "raw" / "diavgeia"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "normalized"

DETAIL_URL = "https://diavgeia.gov.gr/opendata/decisions/{ada}"
MAX_RETRIES = 3
DEFAULT_DELAY = 0.35

# Decision types we care about for spend comparison
DEFAULT_TYPE_UIDS = {
    "Δ.1",    # ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ (direct awards)
    "Δ.2.2",  # ΚΑΤΑΚΥΡΩΣΗ (competitive tender award)
    "Γ.3.4",  # ΣΥΜΒΑΣΗ (contract signing — often contains final value)
}


def fetch_detail(ada: str, timeout: int = 45) -> dict[str, Any]:
    url = DETAIL_URL.format(ada=ada)
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1:
                wait = 10 * (2 ** attempt)
                print(f"    Retry {attempt+1}/{MAX_RETRIES} after {wait}s ({type(exc).__name__})")
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


def decision_cache_path(raw_root: Path, org: str, year: int, month: int, ada: str) -> Path:
    return (
        raw_root
        / f"organization_uid={org}"
        / f"year={year}"
        / f"month={month:02d}"
        / "decisions"
        / f"{ada}.json"
    )


def hydrate_org(
    org: str,
    raw_root: Path,
    type_uids: set[str],
    *,
    dry_run: bool,
    delay: float,
    months_filter: str | None,
) -> dict[str, int]:
    org_dir = raw_root / f"organization_uid={org}"
    if not org_dir.exists():
        print(f"  [skip] no raw data at {org_dir}")
        return {}

    stats = {"scanned": 0, "target": 0, "already_cached": 0, "fetched": 0, "errors": 0}

    search_files = sorted(org_dir.glob("year=*/month=*/search_export.json"))

    # Optional month filter
    if months_filter:
        if ":" in months_filter:
            start, end = months_filter.split(":", 1)
        else:
            start = end = months_filter
        # keep only files within range
        def in_range(p: Path) -> bool:
            parts = p.parts
            y = next((x for x in parts if x.startswith("year=")), "year=0").split("=")[1]
            m = next((x for x in parts if x.startswith("month=")), "month=0").split("=")[1]
            ym = f"{y}-{int(m):02d}"
            return start <= ym <= end
        search_files = [f for f in search_files if in_range(f)]

    for search_path in search_files:
        parts = search_path.parts
        year = int(next(x for x in parts if x.startswith("year=")).split("=")[1])
        month = int(next(x for x in parts if x.startswith("month=")).split("=")[1])

        data = json.loads(search_path.read_text())
        rows = data.get("decisionResultList", data.get("decisions", []))
        stats["scanned"] += len(rows)

        targets = [r for r in rows if r.get("decisionTypeUid", "") in type_uids]
        stats["target"] += len(targets)

        for row in targets:
            ada = str(row.get("ada") or row.get("ADA") or "").strip()
            if not ada:
                continue

            cache_path = decision_cache_path(raw_root, org, year, month, ada)
            if cache_path.exists():
                stats["already_cached"] += 1
                continue

            type_uid = row.get("decisionTypeUid", "")
            subject = str(row.get("subject", ""))[:60]
            print(f"  [{year}-{month:02d}] {type_uid} {ada} — {subject}")

            if dry_run:
                stats["fetched"] += 1
                continue

            try:
                detail = fetch_detail(ada)
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(detail, ensure_ascii=False, indent=2))
                stats["fetched"] += 1
                time.sleep(delay)
            except Exception as exc:
                print(f"    ERROR: {exc}")
                stats["errors"] += 1

    return stats


def run_post_hydration(org: str, output_root: Path, *, dry_run: bool) -> None:
    """Re-run normalize + lifecycle so amounts land in CSVs."""
    python = sys.executable

    steps = [
        (
            "Normalize",
            [python, str(SCRIPTS_DIR / "build_normalized_tables.py"),
             "--org", org,
             "--raw-root", str(DEFAULT_RAW_ROOT),
             "--output-root", str(output_root),
             "--format", "csv"],
        ),
        (
            "Lifecycle linking",
            [python, str(SCRIPTS_DIR / "link_procurement_lifecycle.py"),
             "--org", org,
             "--input-dir", str(output_root)],
        ),
    ]

    for label, cmd in steps:
        print(f"\n  → {label}")
        printable = " ".join(str(c) for c in cmd)
        print(f"    $ {printable}")
        if dry_run:
            print("    [dry-run]")
            continue
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            print(f"    [exit {result.returncode}]")


def main() -> int:
    parser = argparse.ArgumentParser(description="Targeted hydration by decision type.")
    parser.add_argument("--orgs", nargs="+", required=True, help="Org UIDs to hydrate")
    parser.add_argument(
        "--type-uids", nargs="+",
        default=sorted(DEFAULT_TYPE_UIDS),
        help=f"Decision type UIDs to hydrate (default: {sorted(DEFAULT_TYPE_UIDS)})",
    )
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--months", default=None, help="Month range e.g. 2020-01:2026-05")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip post-hydration normalize+lifecycle")
    args = parser.parse_args()

    type_uids = set(args.type_uids)
    print(f"Target type UIDs: {sorted(type_uids)}")
    print(f"Orgs: {args.orgs}")
    print(f"Dry-run: {args.dry_run}")
    print()

    total_fetched = 0
    for org in args.orgs:
        print(f"{'='*60}")
        print(f"  org={org}")
        print(f"{'='*60}")
        stats = hydrate_org(
            org,
            args.raw_root,
            type_uids,
            dry_run=args.dry_run,
            delay=args.delay,
            months_filter=args.months,
        )
        fetched = stats.get("fetched", 0)
        total_fetched += fetched
        print(
            f"\n  Scanned: {stats.get('scanned',0):,}  "
            f"Target-type: {stats.get('target',0):,}  "
            f"Already cached: {stats.get('already_cached',0):,}  "
            f"Fetched: {fetched:,}  "
            f"Errors: {stats.get('errors',0):,}"
        )

        if not args.skip_normalize and not args.dry_run and fetched > 0:
            run_post_hydration(org, args.output_root, dry_run=args.dry_run)

    print(f"\nTotal fetched: {total_fetched:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
