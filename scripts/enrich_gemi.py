#!/usr/bin/env python3
"""Enrich supplier records with ΓΕΜΗ (Greek General Commercial Registry) company data.

Looks up each unique supplier tax ID in the ΓΕΜΗ OpenData API and appends
registration details (legal name, legal form, share capital, registration date,
status, address, activity codes) to a gemi_enrichment.csv alongside the existing
supplier tables.

API: https://opendata-api.businessportal.gr/api/opendata/v1/companies?afm=<AFM>
Auth: free registration at https://opendata.businessportal.gr/register/
      — pass the API key via --api-key or the GEMI_API_KEY environment variable.

Usage:
    python scripts/enrich_gemi.py --org 6166 --api-key YOUR_KEY
    GEMI_API_KEY=xxx python scripts/enrich_gemi.py --org 6166
    python scripts/enrich_gemi.py --org 6166 --api-key KEY --dry-run
    python scripts/enrich_gemi.py --org 6166 --api-key KEY --tax-ids 094275308,011278629
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = REPO_ROOT / "data" / "normalized"
GEMI_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"
GEMI_API_HEADER = "api-key"          # Kong gateway header name
GEMI_RATE_LIMIT_DELAY = 8.0          # 8 req/min → 7.5s min; use 8s to be safe

# Columns written to gemi_enrichment.csv
GEMI_COLUMNS = [
    "supplier_tax_id",
    "gemi_number",
    "legal_name",
    "distinctive_title",
    "legal_form",
    "legal_form_id",
    "status",
    "is_active",
    "registration_date",
    "share_capital",
    "address_street",
    "address_number",
    "address_city",
    "address_postal_code",
    "address_prefecture",
    "primary_activity_code",
    "primary_activity_description",
    "gemi_office",
    "board_members",          # pipe-separated "Name (Role)" strings
    "lookup_status",          # "found" | "not_found" | "error" | "no_key"
    "lookup_error",
]

# Transparency flags — appended as analysis columns
FLAG_COLUMNS = [
    "flag_low_capital",           # capital < 10k and contract value > 100k
    "flag_recently_registered",   # registered within 12 months of first contract
    "flag_inactive",              # status is not active at time of enrichment
    "flag_no_gemi_record",        # no record found in ΓΕΜΗ
]


def lookup_company(tax_id: str, api_key: str, *, timeout: int = 15) -> dict[str, Any]:
    """Return enriched company dict for a Greek AFM. On error, returns a dict with lookup_status='error'."""
    afm = tax_id.zfill(9)
    url = f"{GEMI_BASE}/companies"
    headers = {GEMI_API_HEADER: api_key, "Accept": "application/json"}
    params = {"afm": afm, "resultsSize": 1}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 401:
            return {"lookup_status": "no_key", "lookup_error": "Invalid or missing API key"}
        if r.status_code == 404:
            return {"lookup_status": "not_found", "lookup_error": ""}
        if r.status_code == 429:
            return {"lookup_status": "error", "lookup_error": "Rate limited (429)"}
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        return {"lookup_status": "error", "lookup_error": str(exc)[:120]}

    # Actual API response: {"searchMetadata": {...}, "searchResults": [...]}
    results = (
        data.get("searchResults")
        or data.get("companies")
        or data.get("results")
        or (data if isinstance(data, list) else [])
    )
    if not results:
        return {"lookup_status": "not_found", "lookup_error": ""}

    c = results[0]

    # Activities: list of {activity: {id, descr, kadVersion}, dtFrom, dtTo, type}
    activities = c.get("activities") or []
    primary_act = next(
        (a for a in activities if a.get("type") == "Κύρια" and not a.get("dtTo")),
        next((a for a in activities if a.get("type") == "Κύρια"), activities[0] if activities else None),
    )
    primary_code = (primary_act or {}).get("activity", {}).get("id") or ""
    primary_desc = (primary_act or {}).get("activity", {}).get("descr") or ""

    # Capital: list of {capitalStock, currency, ...}
    capital_list = c.get("capital") or []
    share_capital = capital_list[0].get("capitalStock", "") if capital_list else ""

    # Status: {"id": N, "descr": "Ενεργή"}
    status_val = (c.get("status") or {}).get("descr") or ""
    is_active = "true" if status_val == "Ενεργή" else "false"

    # Board members: persons with active mandates
    persons = c.get("persons") or []
    board = [
        f"{p['personName'].strip()} ({p['role']})"
        for p in persons
        if not p.get("dtTo") or p.get("dtTo", "") > "2026"
    ]

    return {
        "gemi_number": c.get("arGemi") or "",
        "legal_name": c.get("coNameEl") or "",
        "distinctive_title": (c.get("coTitlesEl") or [""])[0],
        "legal_form": (c.get("legalType") or {}).get("descr") or "",
        "legal_form_id": (c.get("legalType") or {}).get("id") or "",
        "status": status_val,
        "is_active": is_active,
        "registration_date": c.get("incorporationDate") or "",
        "share_capital": share_capital,
        "address_street": c.get("street") or "",
        "address_number": c.get("streetNumber") or "",
        "address_city": c.get("city") or (c.get("municipality") or {}).get("descr") or "",
        "address_postal_code": c.get("zipCode") or "",
        "address_prefecture": (c.get("prefecture") or {}).get("descr") or "",
        "primary_activity_code": primary_code,
        "primary_activity_description": primary_desc,
        "gemi_office": (c.get("gemiOffice") or {}).get("descr") or "",
        "board_members": " | ".join(board),
        "lookup_status": "found",
        "lookup_error": "",
    }


def compute_flags(
    row: dict[str, Any],
    supplier_row: dict[str, Any],
) -> dict[str, str]:
    """Compute transparency flags from ΓΕΜΗ data + supplier spend."""
    flags: dict[str, str] = {f: "0" for f in FLAG_COLUMNS}

    if row.get("lookup_status") == "not_found":
        flags["flag_no_gemi_record"] = "1"
        return flags

    if row.get("is_active", "") not in ("true", ""):
        flags["flag_inactive"] = "1"

    # Low capital: share capital < €10,000 and total procurement > €100,000
    try:
        capital = float(str(row.get("share_capital") or "0").replace(",", ".") or "0")
        total_spend = float(supplier_row.get("total_amount") or "0")
        if capital < 10_000 and total_spend > 100_000:
            flags["flag_low_capital"] = "1"
    except (TypeError, ValueError):
        pass

    # Recently registered: registration within 12 months of supplier's first contract
    reg_date = row.get("registration_date") or ""
    first_seen = supplier_row.get("first_seen") or ""
    if reg_date and first_seen:
        try:
            from datetime import datetime
            reg = datetime.fromisoformat(reg_date[:10])
            first = datetime.fromisoformat(first_seen[:10])
            delta_days = (first - reg).days
            if 0 <= delta_days <= 365:
                flags["flag_recently_registered"] = "1"
        except (ValueError, TypeError):
            pass

    return flags


def load_suppliers(input_dir: Path, org: str) -> list[dict[str, Any]]:
    path = input_dir / f"org={org}" / "suppliers.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_existing_enrichment(path: Path) -> dict[str, dict]:
    """Load already-enriched tax IDs to allow resuming interrupted runs."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8", newline="") as f:
        return {r["supplier_tax_id"]: r for r in csv.DictReader(f) if r.get("supplier_tax_id")}


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich suppliers with ΓΕΜΗ company data.")
    parser.add_argument("--org", required=True)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--api-key", default=os.environ.get("GEMI_API_KEY", ""))
    parser.add_argument("--tax-ids", default=None, help="Comma-separated tax IDs to process (default: all suppliers)")
    parser.add_argument("--delay", type=float, default=GEMI_RATE_LIMIT_DELAY, help=f"Seconds between API calls (default {GEMI_RATE_LIMIT_DELAY} — respects 8 req/min limit)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Re-lookup already-enriched records")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not args.api_key and not args.dry_run:
        print(
            "No ΓΕΜΗ API key found.\n"
            "  Register for free at: https://opendata.businessportal.gr/register/\n"
            "  Then pass: --api-key YOUR_KEY  or  export GEMI_API_KEY=YOUR_KEY\n"
            "  (Use --dry-run to preview without a key)"
        )
        return 1

    suppliers = load_suppliers(args.input_dir, args.org)
    if not suppliers:
        print(f"No suppliers.csv found under {args.input_dir}/org={args.org}/")
        return 1

    # Build tax ID list
    supplier_by_tax: dict[str, dict] = {}
    for s in suppliers:
        tid = (s.get("supplier_tax_id") or "").strip()
        if tid and tid not in supplier_by_tax:
            supplier_by_tax[tid] = s

    if args.tax_ids:
        target_ids = [t.strip() for t in args.tax_ids.split(",") if t.strip()]
    else:
        target_ids = list(supplier_by_tax.keys())

    out_path = args.input_dir / f"org={args.org}" / "gemi_enrichment.csv"
    existing = {} if args.force else load_existing_enrichment(out_path)

    to_process = [tid for tid in target_ids if tid not in existing]
    print(f"Suppliers with tax IDs: {len(target_ids)}")
    print(f"Already enriched: {len(existing)}")
    print(f"To fetch: {len(to_process)}")

    if args.dry_run:
        print("[dry-run] would look up:", to_process[:5], "..." if len(to_process) > 5 else "")
        return 0

    all_rows = dict(existing)
    found = not_found = errors = 0

    for i, tid in enumerate(to_process, 1):
        supplier_row = supplier_by_tax.get(tid, {})
        if args.verbose:
            print(f"  [{i}/{len(to_process)}] {tid} ...", end=" ", flush=True)

        gemi_row = lookup_company(tid, args.api_key)

        if gemi_row.get("lookup_status") == "no_key":
            print(f"\nAPI key rejected: {gemi_row['lookup_error']}")
            return 1

        flags = compute_flags(gemi_row, supplier_row)
        row = {"supplier_tax_id": tid, **{k: gemi_row.get(k, "") for k in GEMI_COLUMNS[1:]}, **flags}
        all_rows[tid] = row

        status = gemi_row.get("lookup_status", "error")
        if status == "found":
            found += 1
            if args.verbose:
                print(f"✓ {row['legal_name'][:50]}")
        elif status == "not_found":
            not_found += 1
            if args.verbose:
                print("not found")
        else:
            errors += 1
            if args.verbose:
                print(f"ERROR: {gemi_row.get('lookup_error','')}")

        if i % 50 == 0:
            _write_enrichment(out_path, list(all_rows.values()))
            print(f"  Progress checkpoint: {i}/{len(to_process)} ({found} found, {not_found} not found, {errors} errors)")

        time.sleep(args.delay)

    _write_enrichment(out_path, list(all_rows.values()))

    print(f"\nDone: {found} found, {not_found} not found, {errors} errors")
    print(f"Output: {out_path}")

    # Transparency flag summary
    flags_data = list(all_rows.values())
    for flag in FLAG_COLUMNS:
        count = sum(1 for r in flags_data if r.get(flag) == "1")
        if count:
            print(f"  ⚑  {flag}: {count} supplier(s)")

    return 0


def _write_enrichment(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    all_cols = GEMI_COLUMNS + FLAG_COLUMNS
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
