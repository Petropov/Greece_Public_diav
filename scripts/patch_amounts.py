#!/usr/bin/env python3
"""Patch decisions.csv and procurements.csv with amounts and supplier info
extracted directly from hydrated decision JSONs using type-specific rules.

The generic normalization pipeline misses two key patterns:
  - Δ.1 (direct award): awardAmount.amount + person[].{name, afm}
  - Β.2.2 (payment finalization): sponsor[].expenseAmount.amount + sponsor[].sponsorAFMName
  - Β.1.3 / Β.2.1: amountWithVAT.amount
  - Γ.3.4 (personal contract): contractAmount.amount + person[].{name, afm}

Usage:
    python scripts/patch_amounts.py --org 6166
    python scripts/patch_amounts.py --org 6166 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _amount(obj) -> float | None:
    """Extract numeric amount from a value or {amount: X} dict."""
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return float(obj) if obj == obj else None
    if isinstance(obj, dict):
        v = obj.get("amount")
        if v is not None:
            return _amount(v)
    if isinstance(obj, str):
        s = obj.strip().replace("€", "").replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _name_clean(raw: str | None) -> str | None:
    if not raw:
        return None
    # Diavgeia sometimes stores names as "SURNAME,,FIRST,MIDDLE"
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return " ".join(parts) or None


def extract_from_detail(detail: dict) -> dict:
    """Type-aware extraction of amount and supplier from a hydrated decision JSON."""
    ev = detail.get("extraFieldValues") or {}
    dtype = detail.get("decisionTypeId", "")

    amount = None
    supplier_name = None
    supplier_afm = None

    # --- Δ.1 / Δ.2.2 : direct award / contract award ---
    if dtype in ("Δ.1", "Δ.2.2"):
        amount = _amount(ev.get("awardAmount"))
        persons = ev.get("person") or []
        if isinstance(persons, dict):
            persons = [persons]
        for p in persons:
            if isinstance(p, dict):
                afm = p.get("afm") or p.get("AFM")
                name = _name_clean(p.get("name"))
                if afm and re.fullmatch(r"\d{9}", str(afm).strip()):
                    supplier_afm = str(afm).strip()
                if name:
                    supplier_name = name
                if supplier_afm and supplier_name:
                    break

    # --- Β.2.2 : payment finalization (ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ) ---
    elif dtype == "Β.2.2":
        sponsors = ev.get("sponsor") or []
        if isinstance(sponsors, dict):
            sponsors = [sponsors]
        total = 0.0
        has_amount = False
        for sp in sponsors:
            if not isinstance(sp, dict):
                continue
            a = _amount(sp.get("expenseAmount"))
            if a is not None:
                total += a
                has_amount = True
            if supplier_name is None:
                afm_obj = sp.get("sponsorAFMName") or {}
                if isinstance(afm_obj, dict):
                    afm = afm_obj.get("afm") or afm_obj.get("AFM")
                    name = _name_clean(afm_obj.get("name"))
                    if afm and re.fullmatch(r"\d{9}", str(afm).strip()):
                        supplier_afm = str(afm).strip()
                    if name:
                        supplier_name = name
        if has_amount:
            amount = total

    # --- Β.2.1 : expenditure approval (ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ) ---
    elif dtype == "Β.2.1":
        sponsors = ev.get("sponsor") or []
        if isinstance(sponsors, dict):
            sponsors = [sponsors]
        total = 0.0
        has_amount = False
        for sp in sponsors:
            if not isinstance(sp, dict):
                continue
            a = _amount(sp.get("expenseAmount"))
            if a is not None:
                total += a
                has_amount = True
            if supplier_name is None:
                afm_obj = sp.get("sponsorAFMName") or {}
                if isinstance(afm_obj, dict):
                    afm = afm_obj.get("afm") or afm_obj.get("AFM")
                    name = _name_clean(afm_obj.get("name"))
                    if afm and re.fullmatch(r"\d{9}", str(afm).strip()):
                        supplier_afm = str(afm).strip()
                    if name:
                        supplier_name = name
        if has_amount:
            amount = total

    # --- Β.1.3 : payment warrant (ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ) ---
    elif dtype == "Β.1.3":
        amount = _amount(ev.get("amountWithVAT"))

    # --- Γ.3.4 : personal contract ---
    elif dtype == "Γ.3.4":
        amount = _amount(ev.get("contractAmount"))
        persons = ev.get("person") or []
        if isinstance(persons, dict):
            persons = [persons]
        for p in persons:
            if isinstance(p, dict):
                afm = p.get("afm") or p.get("AFM")
                name = _name_clean(p.get("name"))
                if afm and re.fullmatch(r"\d{9}", str(afm).strip()):
                    supplier_afm = str(afm).strip()
                if name:
                    supplier_name = name
                if supplier_afm and supplier_name:
                    break

    # --- Fallback: try common nested amount fields ---
    if amount is None:
        for key in ("awardAmount", "contractAmount", "amountWithVAT", "estimatedAmount"):
            v = ev.get(key)
            if v is not None:
                amount = _amount(v)
                if amount is not None:
                    break

    return {
        "amount": amount,
        "supplier_name": supplier_name,
        "supplier_tax_id": supplier_afm,
    }


def build_patch(org: str, raw_root: Path) -> dict[str, dict]:
    """Return dict of {ada: {amount, supplier_name, supplier_tax_id}} for all hydrated decisions."""
    org_dir = raw_root / f"organization_uid={org}"
    if not org_dir.exists():
        # Also check legacy flat path
        org_dir = raw_root.parent / org
        if not org_dir.exists():
            raise FileNotFoundError(f"No raw data for org {org}")

    patches: dict[str, dict] = {}

    # Path 1: structured monthly layout
    detail_dirs = list(org_dir.glob("year=*/month=*/decisions/"))
    # Path 2: legacy flat layout
    flat_dir = raw_root.parent / org / "decisions"
    if flat_dir.exists():
        detail_dirs = [flat_dir]

    for detail_dir in detail_dirs:
        for json_path in detail_dir.glob("*.json"):
            try:
                detail = load_json(json_path)
                ada = detail.get("ada") or json_path.stem
                result = extract_from_detail(detail)
                # Only record if we got something useful
                if result["amount"] is not None or result["supplier_name"] or result["supplier_tax_id"]:
                    patches[ada] = result
            except Exception:
                continue

    return patches


def apply_patch(csv_path: Path, patches: dict[str, dict], dry_run: bool = False) -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    original_amount_count = df["amount"].notna().sum() if "amount" in df.columns else 0

    patched = 0
    for idx, row in df.iterrows():
        ada = row.get("ada") or row.get("contract_id") or row.get("ada")
        if not ada or ada not in patches:
            continue
        p = patches[ada]

        # Patch amount if missing
        if pd.isna(df.at[idx, "amount"]) or df.at[idx, "amount"] == 0:
            if p["amount"] is not None and p["amount"] > 0:
                df.at[idx, "amount"] = p["amount"]
                patched += 1

        # Patch supplier name if missing
        if "supplier_name" in df.columns:
            cur_name = df.at[idx, "supplier_name"]
            if (pd.isna(cur_name) or str(cur_name).strip() == "") and p["supplier_name"]:
                df.at[idx, "supplier_name"] = p["supplier_name"]

        # Patch supplier name_raw if exists (contracts.csv uses this)
        if "supplier_name_raw" in df.columns:
            cur = df.at[idx, "supplier_name_raw"]
            if (pd.isna(cur) or str(cur).strip() == "") and p["supplier_name"]:
                df.at[idx, "supplier_name_raw"] = p["supplier_name"]

        # Patch supplier tax id if missing
        if "supplier_tax_id" in df.columns:
            cur_tid = df.at[idx, "supplier_tax_id"]
            if (pd.isna(cur_tid) or str(cur_tid).strip() == "") and p["supplier_tax_id"]:
                df.at[idx, "supplier_tax_id"] = p["supplier_tax_id"]

    new_amount_count = df["amount"].notna().sum() if "amount" in df.columns else 0
    print(f"  {csv_path.name}: amount coverage {original_amount_count} → {new_amount_count} (+{new_amount_count - original_amount_count}), rows patched: {patched}")

    if not dry_run:
        df.to_csv(csv_path, index=False)
    return df


def structuring_report(df: pd.DataFrame) -> None:
    """Print structuring candidates: same supplier, multiple direct awards, amounts clustered under 30k."""
    df = df.copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Filter to direct awards with amounts
    awards = df[
        df.get("decision_type", pd.Series(dtype=str)).str.contains("ΑΝΑΘΕΣΗ|ΑΝΑΛ", na=False) |
        df.get("decision_type", pd.Series(dtype=str)).str.contains("Δ.1", na=False)
    ].copy() if "decision_type" in df.columns else df.copy()

    awards = awards[awards["amount"].notna() & (awards["amount"] > 0)]

    if awards.empty:
        print("\n  No direct awards with amounts found for structuring analysis.")
        return

    name_col = "supplier_name" if "supplier_name" in awards.columns else "supplier_name_raw"
    tid_col = "supplier_tax_id" if "supplier_tax_id" in awards.columns else None

    print(f"\n  === Amount distribution (direct awards, n={len(awards)}) ===")
    bins = [(0, 1000), (1000, 10000), (10000, 20000), (20000, 30000), (30000, 140000), (140000, float("inf"))]
    labels = ["<€1k", "€1k–10k", "€10k–20k", "€20k–30k (structuring zone)", "€30k–140k", ">€140k"]
    for (lo, hi), label in zip(bins, labels):
        count = ((awards["amount"] >= lo) & (awards["amount"] < hi)).sum()
        pct = 100 * count / len(awards)
        print(f"    {label:<35} {count:>5}  ({pct:.1f}%)")

    print(f"\n  === Structuring candidates (supplier with 2+ awards, any under €30k) ===")
    under30 = awards[awards["amount"] < 30000]
    if under30.empty:
        print("  None found.")
        return

    if name_col in under30.columns:
        key_col = tid_col if tid_col and tid_col in under30.columns else name_col
        groups = under30.groupby(key_col).agg(
            count=("amount", "count"),
            total=("amount", "sum"),
            max_single=("amount", "max"),
            name=(name_col, "first") if name_col != key_col else ("amount", "count"),
        ).sort_values("count", ascending=False)
        multi = groups[groups["count"] >= 2]
        if multi.empty:
            print("  No supplier has 2+ direct awards under €30k (in the data with amounts).")
        else:
            print(f"  Found {len(multi)} suppliers with repeated direct awards under €30k:")
            print(multi.head(20).to_string())


def main():
    parser = argparse.ArgumentParser(description="Patch normalized CSVs with amounts from raw JSONs")
    parser.add_argument("--org", required=True, help="Organization UID")
    parser.add_argument("--dry-run", action="store_true", help="Show stats without writing")
    parser.add_argument("--report", action="store_true", default=True, help="Print structuring report after patching")
    args = parser.parse_args()

    raw_root = REPO_ROOT / "data" / "raw" / "diavgeia"
    norm_root = REPO_ROOT / "data" / "normalized" / f"org={args.org}"

    # Also check legacy path
    legacy_detail = REPO_ROOT / "data" / args.org / "decisions"
    if legacy_detail.exists():
        print(f"Found legacy flat detail path: {legacy_detail}")

    print(f"\nBuilding patch index for org {args.org}...")
    try:
        patches = build_patch(args.org, raw_root)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    print(f"Hydrated decisions with useful data: {len(patches)}")

    if not norm_root.exists():
        print(f"No normalized data at {norm_root}")
        return

    print(f"\nApplying patches ({'DRY RUN' if args.dry_run else 'WRITING'})...")
    patched_df = None
    for csv_name in ("decisions.csv", "procurements.csv", "contracts.csv"):
        csv_path = norm_root / csv_name
        if csv_path.exists():
            df = apply_patch(csv_path, patches, dry_run=args.dry_run)
            if csv_name == "decisions.csv":
                patched_df = df

    if args.report and patched_df is not None:
        structuring_report(patched_df)


if __name__ == "__main__":
    main()
