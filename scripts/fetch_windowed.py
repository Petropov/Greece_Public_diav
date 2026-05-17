#!/usr/bin/env python3
"""Re-fetch capped months using weekly date windows to bypass the 500-ADA limit.

The Diavgeia search/export API returns at most 500 unique decisions per query.
Months with >500 decisions appear capped (the cache holds exactly 500 unique ADAs).
This script re-fetches those months by splitting each into weekly sub-windows,
merging and deduplicating the results, and writing them back to search_export.json.

Usage:
    # Re-fetch all capped months for org 6166
    python scripts/fetch_windowed.py --org 6166

    # Re-fetch a specific range
    python scripts/fetch_windowed.py --org 6166 --months 2022-01:2024-12

    # Preview without fetching
    python scripts/fetch_windowed.py --org 6166 --dry-run

    # Use smaller windows (daily) for very high-volume months
    python scripts/fetch_windowed.py --org 6166 --window-days 1
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = REPO_ROOT / "data" / "raw" / "diavgeia"
BASE_URL = "https://diavgeia.gov.gr/luminapi/api/search/export"

CAP_THRESHOLD = 490  # treat month as capped if unique ADAs >= this


def month_windows(year: int, month: int, window_days: int = 7) -> list[tuple[date, date]]:
    """Split a calendar month into non-overlapping windows of `window_days` days."""
    _, last_day = monthrange(year, month)
    windows = []
    cur = date(year, month, 1)
    end_of_month = date(year, month, last_day)
    while cur <= end_of_month:
        win_end = min(cur + timedelta(days=window_days - 1), end_of_month)
        windows.append((cur, win_end))
        cur = win_end + timedelta(days=1)
    return windows


def fetch_window(
    org: str,
    start: date,
    end: date,
    *,
    delay: float = 0.5,
    max_retries: int = 3,
) -> list[dict]:
    """Fetch all decisions for org in [start, end] (may make multiple page requests)."""
    query = (
        f'organizationUid:"{org}" AND '
        f"issueDate:[DT({start.isoformat()}T00:00:00) TO DT({end.isoformat()}T23:59:59)]"
    )
    rows: list[dict] = []
    page = 0
    while True:
        params = {
            "q": query,
            "sort": "recent",
            "wt": "json",
            "page": page,
            "size": 500,
        }
        for attempt in range(1, max_retries + 1):
            try:
                qs = urlencode(params)
                script = (
                    "import requests,json,sys;"
                    f"r=requests.get({BASE_URL!r},params={params!r},timeout=(15,30));"
                    "print(r.status_code);print(r.text)"
                )
                result = subprocess.run(
                    [sys.executable, "-c", script],
                    capture_output=True, text=True, timeout=45,
                )
                if result.returncode != 0:
                    raise RuntimeError(result.stderr[:200] or "subprocess failed")
                lines = result.stdout.split("\n", 1)
                status_code = int(lines[0].strip())
                text = lines[1] if len(lines) > 1 else ""
                if status_code == 404:
                    batch = []
                    break
                if status_code >= 400:
                    raise RuntimeError(f"HTTP {status_code}")
                batch = json.loads(text).get("decisionResultList", [])
                break
            except subprocess.TimeoutExpired:
                raise TimeoutError("Request timed out (45s hard limit)")
            except TimeoutError:
                raise  # never retry timeouts
            except Exception as exc:
                if attempt == max_retries:
                    raise
                wait = 10 * attempt  # 10s, 20s between retries
                print(f"    Retry {attempt}/{max_retries} after {wait}s ({exc})")
                time.sleep(wait)
        rows.extend(batch)
        if len(batch) < 500:
            break
        page += 1
        time.sleep(delay)
    return rows


def unique_ada_count(export_path: Path) -> int:
    if not export_path.exists():
        return 0
    data = json.loads(export_path.read_text(encoding="utf-8"))
    rows = data.get("decisionResultList", [])
    return len(set(d.get("ada", "") for d in rows if d.get("ada")))


def refetch_month(
    org: str,
    year: int,
    month: int,
    raw_root: Path,
    *,
    window_days: int = 7,
    delay: float = 0.5,
    dry_run: bool = False,
    force: bool = False,
    verbose: bool = False,
) -> dict:
    month_dir = raw_root / f"organization_uid={org}" / f"year={year}" / f"month={month:02d}"
    export_path = month_dir / "search_export.json"
    meta_path = month_dir / "fetch_metadata.json"

    current_count = unique_ada_count(export_path)
    label = f"{year}-{month:02d}"

    if current_count < CAP_THRESHOLD and not force:
        if verbose:
            print(f"  {label}: {current_count} ADAs — not capped, skipping")
        return {"month": label, "status": "skipped_not_capped", "before": current_count}

    windows = month_windows(year, month, window_days)
    print(f"  {label}: {current_count} ADAs capped → re-fetching with {len(windows)} windows of {window_days}d each", flush=True)

    if dry_run:
        print(f"    [dry-run] would fetch {len(windows)} windows")
        return {"month": label, "status": "dry_run", "before": current_count}

    # Seed all_rows with the EXISTING cache — windowed fetch adds to it, never replaces
    all_rows: dict[str, dict] = {}
    if export_path.exists():
        try:
            existing_data = json.loads(export_path.read_text(encoding="utf-8"))
            for row in existing_data.get("decisionResultList", []):
                ada = row.get("ada", "")
                if ada:
                    all_rows[ada] = row
        except Exception:
            pass

    api_calls = 0
    errors = 0
    for win_start, win_end in windows:
        # Diavgeia server consistently hangs on Dec 29-31 queries; skip to avoid pipe deadlock
        if win_start.month == 12 and win_start.day >= 29:
            print(f"    {win_start}–{win_end}: skipped (known server hang for Dec 29-31)", flush=True)
            continue
        try:
            batch = fetch_window(org, win_start, win_end, delay=delay)
            api_calls += 1
            added = 0
            for row in batch:
                ada = row.get("ada", "")
                if ada and ada not in all_rows:
                    all_rows[ada] = row
                    added += 1
            if verbose:
                print(f"    {win_start}–{win_end}: {len(batch)} rows (+{added} new), running unique={len(all_rows)}", flush=True)
            # Write after each window so partial results survive failures
            month_dir.mkdir(parents=True, exist_ok=True)
            export_path.write_text(
                json.dumps({"decisionResultList": list(all_rows.values())}, ensure_ascii=False),
                encoding="utf-8",
            )
            time.sleep(delay)
        except Exception as exc:
            errors += 1
            print(f"    ERROR fetching {win_start}–{win_end}: {exc}", flush=True)

    after_count = len(all_rows)
    gained = after_count - current_count
    print(f"    → {after_count} unique ADAs (+{gained} vs original cache)", flush=True)

    # Write final merged export (already written after each window, but ensure latest)
    month_dir.mkdir(parents=True, exist_ok=True)
    export_path.write_text(
        json.dumps({"decisionResultList": list(all_rows.values())}, ensure_ascii=False),
        encoding="utf-8",
    )

    # Update metadata
    meta: dict = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    meta["windowed_refetch"] = {
        "window_days": window_days,
        "windows": len(windows),
        "api_calls": api_calls,
        "unique_adas_before": current_count,
        "unique_adas_after": after_count,
        "gained": gained,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"month": label, "status": "refetched", "before": current_count, "after": after_count, "gained": gained}


def parse_month_range(spec: str | None) -> list[tuple[int, int]] | None:
    """Parse '2022-01:2024-12' into list of (year, month) tuples. None = all months."""
    if not spec:
        return None
    start_str, _, end_str = spec.partition(":")
    def parse_ym(s: str) -> tuple[int, int]:
        y, m = s.split("-")
        return int(y), int(m)
    if not end_str:
        return [parse_ym(start_str)]
    start_ym = parse_ym(start_str)
    end_ym = parse_ym(end_str)
    result = []
    y, m = start_ym
    while (y, m) <= end_ym:
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def find_all_months(raw_root: Path, org: str) -> list[tuple[int, int]]:
    org_dir = raw_root / f"organization_uid={org}"
    months = []
    for year_dir in sorted(org_dir.glob("year=*")):
        year = int(year_dir.name.split("=")[1])
        for month_dir in sorted(year_dir.glob("month=*")):
            month = int(month_dir.name.split("=")[1])
            if (month_dir / "search_export.json").exists():
                months.append((year, month))
    return months


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-fetch capped months using weekly date windows.")
    parser.add_argument("--org", required=True)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--months", default=None, help="e.g. 2022-01:2024-12 or 2023-06")
    parser.add_argument("--window-days", type=int, default=7, help="Days per window (default 7; use 1 for very busy months)")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between API calls (default 0.5)")
    parser.add_argument("--force", action="store_true", help="Re-fetch even non-capped months")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    target_months = parse_month_range(args.months) if args.months else find_all_months(args.raw_root, args.org)
    if not target_months:
        print("No months found in cache. Run digest_monthly.py --search-only first.")
        return 1

    print(f"Processing {len(target_months)} months for org={args.org}")
    results = []
    for year, month in target_months:
        result = refetch_month(
            args.org, year, month, args.raw_root,
            window_days=args.window_days,
            delay=args.delay,
            dry_run=args.dry_run,
            force=args.force,
            verbose=args.verbose,
        )
        results.append(result)

    refetched = [r for r in results if r["status"] == "refetched"]
    total_gained = sum(r.get("gained", 0) for r in refetched)
    skipped = len([r for r in results if r["status"] == "skipped_not_capped"])

    print(f"\nDone: {len(refetched)} months re-fetched, {skipped} skipped (not capped)")
    print(f"Total new decisions gained: +{total_gained:,}")
    if refetched:
        print("Top gains:")
        for r in sorted(refetched, key=lambda x: -x.get("gained", 0))[:10]:
            print(f"  {r['month']}: {r['before']} → {r['after']} (+{r['gained']})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
