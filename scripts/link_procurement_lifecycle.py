#!/usr/bin/env python3
"""Link procurement decisions across lifecycle stages to produce one row per contract.

Greek public procurement decisions pass through multiple Diavgeia decision types
for the same underlying contract:

  1. ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ / ΠΡΑΞΗ ΠΟΥ ΑΦΟΡΑ ΣΕ ΣΥΛΛΟΓΙΚΟ ΟΡΓΑΝΟ
     — committee minutes approving tender stages (provisional award, evaluation)
  2. ΚΑΤΑΚΥΡΩΣΗ — formal award decision
  3. ΣΥΜΒΑΣΗ — contract signing
  4. ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ — budget commitment
  5. ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ / ΕΝΤΟΛΗ ΠΛΗΡΩΜΗΣ — payment

Because the same contract amount appears in multiple decision rows, naively summing
amounts produces significant double-counting.

This script groups procurement rows by matching:
  - Amount within ±AMOUNT_PCT_TOLERANCE of each other
  - Issue dates within DATE_WINDOW_DAYS of each other
  - Same supplier tax ID where available

Outputs:
  data/normalized/org=<ORG>/contracts.csv   — one row per deduplicated contract
  data/normalized/org=<ORG>/lifecycle.csv   — mapping table (ADA → contract_id)

Usage:
    python scripts/link_procurement_lifecycle.py --org 6166
    python scripts/link_procurement_lifecycle.py --org 6166 --input-dir data/normalized --verbose
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = REPO_ROOT / "data" / "normalized"

AMOUNT_PCT_TOLERANCE = 0.02   # 2% — covers rounding/VAT reclassification across stages
DATE_WINDOW_DAYS = 180        # contract lifecycle rarely spans > 6 months for the same stage group

# Stage priority: which decision type best represents the contract (higher = more authoritative)
STAGE_PRIORITY: dict[str, int] = {
    "ΚΑΤΑΚΥΡΩΣΗ": 10,
    "ΣΥΜΒΑΣΗ": 9,
    "ΕΝΤΟΛΗ ΠΛΗΡΩΜΗΣ": 8,
    "ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ": 7,
    "ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ": 6,
    "ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ": 5,
    "ΠΡΑΞΗ ΠΟΥ ΑΦΟΡΑ ΣΕ ΣΥΛΛΟΓΙΚΟ ΟΡΓΑΝΟ - ΕΠΙΤΡΟΠΗ - ΟΜΑΔΑ ΕΡΓΑΣΙΑΣ - ΟΜΑΔΑ ΕΡΓΟΥ - ΜΕΛΗ ΣΥΛΛΟΓΙΚΟΥ ΟΡΓΑΝΟΥ": 5,
    "ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ": 4,
    "ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ": 8,
    "ΠΕΡΙΛΗΨΗ ΔΙΑΚΗΡΥΞΗΣ": 3,
}


def safe_float(v: Any) -> float:
    try:
        f = float(v)
        return 0.0 if math.isnan(f) else f
    except (TypeError, ValueError):
        return 0.0


def parse_date(s: Any) -> datetime | None:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:len(fmt.replace("%Y","0000").replace("%m","00").replace("%d","00").replace("%H","00").replace("%M","00").replace("%S","00"))], fmt)
        except ValueError:
            pass
    # Unix ms timestamp
    try:
        ts = int(s)
        if ts > 1e10:
            ts //= 1000
        return datetime.utcfromtimestamp(ts)
    except (ValueError, TypeError, OSError):
        return None


def amounts_match(a: float, b: float) -> bool:
    if a <= 0 or b <= 0:
        return False
    diff = abs(a - b) / max(a, b)
    return diff <= AMOUNT_PCT_TOLERANCE


def dates_within(d1: datetime | None, d2: datetime | None) -> bool:
    if d1 is None or d2 is None:
        return True  # can't rule out — be permissive
    return abs((d1 - d2).days) <= DATE_WINDOW_DAYS


def contract_id(amount: float, canonical_date: str, supplier_key: str) -> str:
    key = f"{amount:.2f}|{canonical_date[:7]}|{supplier_key}"
    return "contract:" + hashlib.sha1(key.encode()).hexdigest()[:12]


def load_procurements(input_dir: Path, org: str) -> list[dict]:
    path = input_dir / f"org={org}" / "procurements.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def link_lifecycle(rows: list[dict], *, verbose: bool = False) -> tuple[list[dict], list[dict]]:
    """
    Returns (contracts, lifecycle_map).
    contracts: one row per deduplicated contract (best representative row + metadata).
    lifecycle_map: [{ada, contract_id, stage, amount, issue_date}]
    """
    # Only work with rows that have a structured amount
    with_amount = [r for r in rows if safe_float(r.get("amount")) > 0]
    without_amount = [r for r in rows if safe_float(r.get("amount")) <= 0]

    if verbose:
        print(f"  Rows with amount: {len(with_amount)}, without: {len(without_amount)}")

    # Sort by date for stable grouping
    with_amount.sort(key=lambda r: (parse_date(r.get("issue_date")) or datetime(2000, 1, 1)))

    # Union-Find for grouping
    parent: dict[int, int] = {i: i for i in range(len(with_amount))}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Group rows that share amount ± tolerance and date ± window
    amounts = [safe_float(r.get("amount")) for r in with_amount]
    dates = [parse_date(r.get("issue_date")) for r in with_amount]
    taxes = [r.get("supplier_tax_id") or "" for r in with_amount]

    for i in range(len(with_amount)):
        for j in range(i + 1, len(with_amount)):
            # Early exit: if dates are too far apart, skip
            if dates[i] and dates[j]:
                if abs((dates[i] - dates[j]).days) > DATE_WINDOW_DAYS:
                    continue
            if not amounts_match(amounts[i], amounts[j]):
                continue
            # If both have tax IDs and they don't match, skip
            if taxes[i] and taxes[j] and taxes[i] != taxes[j]:
                continue
            union(i, j)

    # Collect groups
    groups: dict[int, list[int]] = defaultdict(list)
    for i in range(len(with_amount)):
        groups[find(i)].append(i)

    if verbose:
        multi = sum(1 for g in groups.values() if len(g) > 1)
        print(f"  Groups: {len(groups)} total, {multi} multi-stage, {len(groups)-multi} single-stage")

    contracts = []
    lifecycle_map = []

    for group_indices in groups.values():
        group_rows = [with_amount[i] for i in group_indices]

        # Pick the best representative by stage priority
        def stage_rank(r: dict) -> int:
            dt = r.get("decision_type") or ""
            return max((v for k, v in STAGE_PRIORITY.items() if k in dt), default=0)

        best = max(group_rows, key=stage_rank)
        amount = safe_float(best.get("amount"))
        best_date = best.get("issue_date") or ""
        supplier_key = best.get("supplier_key") or best.get("supplier_tax_id") or ""
        cid = contract_id(amount, best_date, supplier_key)

        stages = sorted(set(r.get("decision_type") or "unknown" for r in group_rows))

        contract_row = {
            "contract_id": cid,
            "amount": amount,
            "issue_date": best_date,
            "ada": best.get("ada") or "",
            "decision_type": best.get("decision_type") or "",
            "subject": (best.get("subject") or "")[:120],
            "supplier_key": supplier_key,
            "supplier_tax_id": best.get("supplier_tax_id") or "",
            "supplier_name_raw": (best.get("supplier_name_raw") or "")[:80],
            "stage_count": len(group_rows),
            "stages_seen": "|".join(stages),
            "all_adas": "|".join(r.get("ada") or "" for r in group_rows),
        }
        contracts.append(contract_row)

        for r in group_rows:
            lifecycle_map.append({
                "ada": r.get("ada") or "",
                "contract_id": cid,
                "decision_type": r.get("decision_type") or "",
                "amount": safe_float(r.get("amount")),
                "issue_date": r.get("issue_date") or "",
            })

    # Rows without amounts get their own pass-through contract entries
    for r in without_amount:
        cid = "contract:" + hashlib.sha1((r.get("ada") or "no-ada").encode()).hexdigest()[:12]
        contracts.append({
            "contract_id": cid,
            "amount": 0.0,
            "issue_date": r.get("issue_date") or "",
            "ada": r.get("ada") or "",
            "decision_type": r.get("decision_type") or "",
            "subject": (r.get("subject") or "")[:120],
            "supplier_key": r.get("supplier_key") or r.get("supplier_tax_id") or "",
            "supplier_tax_id": r.get("supplier_tax_id") or "",
            "supplier_name_raw": (r.get("supplier_name_raw") or "")[:80],
            "stage_count": 1,
            "stages_seen": r.get("decision_type") or "unknown",
            "all_adas": r.get("ada") or "",
        })
        lifecycle_map.append({
            "ada": r.get("ada") or "",
            "contract_id": cid,
            "decision_type": r.get("decision_type") or "",
            "amount": 0.0,
            "issue_date": r.get("issue_date") or "",
        })

    contracts.sort(key=lambda c: -safe_float(c.get("amount", 0)))
    return contracts, lifecycle_map


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


CONTRACT_FIELDS = [
    "contract_id", "amount", "issue_date", "ada", "decision_type", "subject",
    "supplier_key", "supplier_tax_id", "supplier_name_raw",
    "stage_count", "stages_seen", "all_adas",
]
LIFECYCLE_FIELDS = ["ada", "contract_id", "decision_type", "amount", "issue_date"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Link procurement decisions across lifecycle stages.")
    parser.add_argument("--org", required=True)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print("Loading procurements...")
    try:
        rows = load_procurements(args.input_dir, args.org)
    except FileNotFoundError as e:
        print(e)
        return 1
    print(f"  {len(rows):,} procurement rows")

    print("Linking lifecycle stages...")
    contracts, lifecycle_map = link_lifecycle(rows, verbose=args.verbose)

    with_amount = [c for c in contracts if safe_float(c.get("amount", 0)) > 0]
    multi_stage = [c for c in contracts if safe_int_local(c.get("stage_count", 1)) > 1]
    total_clean = sum(safe_float(c.get("amount", 0)) for c in with_amount)
    total_raw = sum(safe_float(r.get("amount", 0)) for r in rows)

    print(f"\nResults:")
    print(f"  Raw procurement rows with amounts:    {sum(1 for r in rows if safe_float(r.get('amount',0))>0):>6,}")
    print(f"  Deduplicated contracts:               {len(contracts):>6,}")
    print(f"  Multi-stage (linked across types):    {len(multi_stage):>6,}")
    print(f"  Raw spend sum (with double-counting): {total_raw:>14,.0f} EUR")
    print(f"  Clean spend sum (deduplicated):       {total_clean:>14,.0f} EUR")
    if total_raw > 0:
        reduction = (total_raw - total_clean) / total_raw * 100
        print(f"  Double-counting removed:              {reduction:.1f}%")

    base = args.input_dir / f"org={args.org}"
    contracts_path = base / "contracts.csv"
    lifecycle_path = base / "lifecycle.csv"
    write_csv(contracts_path, contracts, CONTRACT_FIELDS)
    write_csv(lifecycle_path, lifecycle_map, LIFECYCLE_FIELDS)
    print(f"\nWrote: {contracts_path}")
    print(f"Wrote: {lifecycle_path}")

    # Top 20 contracts by value
    print("\nTop 20 contracts by clean amount:")
    print(f"  {'Amount':>14}  {'Date':<12}  {'Type':<20}  Subject")
    for c in with_amount[:20]:
        subj = (c.get("subject") or "")[:55]
        dt = (c.get("decision_type") or "")[:20]
        print(f"  {safe_float(c.get('amount')):>13,.0f}  {c.get('issue_date','')[:10]:<12}  {dt:<20}  {subj}")

    return 0


def safe_int_local(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
