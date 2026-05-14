#!/usr/bin/env python3
"""Robust monthly Diavgeia ingestion for Lamia Municipality.

The pipeline is deliberately split into a lightweight monthly search index and
an optional full-record hydration stage. Raw hydrated JSON is cached unchanged
under data/raw/diavgeia/org=<ORG>/ada=<ADA>.json for auditability.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.lamia_digest import (  # noqa: E402
    BASE_URL,
    LAMIA_ORG_UID,
    build_query,
    decision_url,
    extract_amount,
    extract_decisions,
    extract_supplier_fields,
    first_present,
    is_procurement_decision,
    normalize_date,
    normalize_decision,
)

EXPORT_URL = f"{BASE_URL}/luminapi/api/search/export"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/opendata/decisions/{{ada}}"
RETRY_STATUSES = {429, 500, 502, 503, 504}
INDEX_COLUMNS = [
    "ada",
    "org_uid",
    "issue_date",
    "publish_timestamp",
    "decision_type",
    "subject",
    "url",
    "year",
    "month",
    "fetched_at",
    "source_query",
]
FAILED_COLUMNS = [
    "ADA",
    "URL",
    "month",
    "error_type",
    "error_message",
    "attempt_count",
    "last_attempted_at",
]
COVERAGE_COLUMNS = [
    "org_uid",
    "year",
    "month",
    "indexed_decisions",
    "hydrated_decisions",
    "hydration_pct",
    "procurement_rows",
    "rows_with_amount",
    "amount_extraction_pct",
    "rows_with_supplier",
    "supplier_extraction_pct",
    "exact_500_flag",
    "pagination_complete",
    "failed_hydrations",
    "confidence_rating",
]
FINANCIAL_KEYWORDS = (
    "δαπάνη", "δαπανη", "πληρωμή", "πληρωμη", "ανάθεση", "αναθεση",
    "σύμβαση", "συμβαση", "προμήθεια", "προμηθεια", "τιμολόγ", "τιμολογ",
    "€", "ευρώ", "ευρω", "contract", "payment", "supplier", "award",
)
HYDRATE_TYPE_PREFIXES = ("Β.1", "Β.2", "Δ.")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_month(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM, got {value!r}") from exc


def iter_months(start: date, end: date) -> Iterable[date]:
    current = start.replace(day=1)
    last = end.replace(day=1)
    while current <= last:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def month_bounds(month: date) -> tuple[date, date]:
    if month.month == 12:
        next_month = month.replace(year=month.year + 1, month=1)
    else:
        next_month = month.replace(month=month.month + 1)
    return month, date.fromordinal(next_month.toordinal() - 1)


@dataclass
class MonthAudit:
    year: int
    month: int
    exact_500_flag: bool = False
    pagination_complete: bool = True
    page_count: int = 0
    source_query: str = ""


def request_json(session: requests.Session, url: str, *, params: dict[str, Any] | None,
                 timeout: float, max_retries: int, sleep: float) -> tuple[Any, int]:
    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(url, params=params, timeout=timeout)
            if response.status_code in RETRY_STATUSES and attempt <= max_retries:
                delay = min(30.0, sleep + (2 ** (attempt - 1)) * 0.5 + random.uniform(0, 0.25))
                time.sleep(delay)
                continue
            response.raise_for_status()
            if sleep > 0:
                time.sleep(sleep)
            return response.json(), attempt
        except (ValueError, requests.RequestException):
            if attempt > max_retries:
                raise
            delay = min(30.0, sleep + (2 ** (attempt - 1)) * 0.5 + random.uniform(0, 0.25))
            time.sleep(delay)


def fetch_month_index(session: requests.Session, org: str, month: date, *, size: int,
                      timeout: float, max_retries: int, sleep: float) -> tuple[list[dict[str, Any]], MonthAudit]:
    start, end = month_bounds(month)
    query = build_query(start, end).replace(f'organizationUid:"{LAMIA_ORG_UID}"', f'organizationUid:"{org}"')
    audit = MonthAudit(year=month.year, month=month.month, source_query=query)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    page = 0
    while True:
        params = {"q": query, "sort": "recent", "wt": "json", "page": page, "size": size}
        try:
            payload, _attempts = request_json(session, EXPORT_URL, params=params, timeout=timeout,
                                             max_retries=max_retries, sleep=sleep)
        except Exception as exc:
            audit.pagination_complete = False
            print(f"Warning: failed page {page} for {month:%Y-%m}: {exc}", file=sys.stderr)
            break
        batch = extract_decisions(payload)
        audit.page_count += 1
        if len(batch) == 500:
            audit.exact_500_flag = True
            print(f"Warning: {month:%Y-%m} page {page} returned exactly 500 rows; continuing pagination", file=sys.stderr)
        if not batch:
            break
        for item in batch:
            ada = str(first_present(item, ("ada", "ADA")) or "").strip()
            if ada and ada in seen:
                continue
            if ada:
                seen.add(ada)
            rows.append(item)
        if len(batch) < size:
            break
        page += 1
    return rows, audit


def index_row(item: dict[str, Any], org: str, month: date, fetched_at: str, source_query: str) -> dict[str, Any]:
    norm = normalize_decision(item)
    issue = norm.get("issue_date") or normalize_date(first_present(item, ("issueDate", "publishDate", "submissionTimestamp")))
    return {
        "ada": norm.get("ada"),
        "org_uid": org,
        "issue_date": issue,
        "publish_timestamp": first_present(item, ("publishTimestamp", "submissionTimestamp", "publishDate", "publishedDate")),
        "decision_type": norm.get("decision_type_raw") or norm.get("decision_type"),
        "subject": norm.get("subject"),
        "url": decision_url(norm.get("ada"), item),
        "year": month.year,
        "month": month.month,
        "fetched_at": fetched_at,
        "source_query": json.dumps({"q": source_query, "page_size": 500}, ensure_ascii=False),
    }


def write_table(df: pd.DataFrame, base_path: Path) -> None:
    base_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(base_path.with_suffix(".csv"), index=False)
    try:
        df.to_parquet(base_path.with_suffix(".parquet"), index=False)
    except Exception as exc:
        print(f"Warning: could not write {base_path.with_suffix('.parquet')}: {exc}", file=sys.stderr)


def should_hydrate(row: dict[str, Any], hydrate_all: bool = False) -> bool:
    if hydrate_all:
        return True
    decision_type = str(row.get("decision_type") or "")
    subject = str(row.get("subject") or "").lower()
    if decision_type.startswith(HYDRATE_TYPE_PREFIXES):
        return True
    return any(token in subject for token in FINANCIAL_KEYWORDS)


def cache_path(raw_root: Path, org: str, ada: str) -> Path:
    return raw_root / f"org={org}" / f"ada={ada}.json"


def load_or_fetch_detail(session: requests.Session, raw_root: Path, org: str, ada: str, *,
                         force_refresh: bool, timeout: float, max_retries: int, sleep: float) -> tuple[dict[str, Any], int, str]:
    path = cache_path(raw_root, org, ada)
    if path.exists() and not force_refresh:
        return json.loads(path.read_text(encoding="utf-8")), 0, "cache"
    url = DETAIL_URL_TEMPLATE.format(ada=quote(ada, safe=""))
    payload, attempts = request_json(session, url, params=None, timeout=timeout, max_retries=max_retries, sleep=sleep)
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected detail payload type: {type(payload).__name__}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, attempts, "api"


def unwrap_detail(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("decision", "decisionResult", "data"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def hydrate_index(index_df: pd.DataFrame, org: str, args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    failures: list[dict[str, Any]] = []
    hydrated: list[dict[str, Any]] = []
    with requests.Session() as session:
        for row in index_df.to_dict("records"):
            ada = str(row.get("ada") or "").strip()
            if not ada or not should_hydrate(row, args.hydrate_all):
                continue
            attempts = 0
            try:
                payload, attempts, status = load_or_fetch_detail(
                    session, args.raw_root, org, ada, force_refresh=args.force_refresh,
                    timeout=args.timeout, max_retries=args.max_retries, sleep=args.sleep,
                )
                detail = unwrap_detail(payload)
                merged = {**row, **detail} if isinstance(detail, dict) else dict(row)
                norm = normalize_decision(merged)
                amount, amount_source = extract_amount(detail)
                supplier_name, supplier_afm, supplier_source, supplier_afm_source = extract_supplier_fields(detail)
                hydrated.append({
                    **row,
                    "amount": norm.get("amount") if norm.get("amount") is not None else amount,
                    "amount_source": norm.get("amount_source") or (f"raw:{amount_source}" if amount_source else None),
                    "supplier_name": norm.get("supplier_name") or supplier_name,
                    "supplier_afm": norm.get("supplier_tax_id") or supplier_afm,
                    "supplier_source": norm.get("supplier_name_source") or supplier_source,
                    "supplier_afm_source": norm.get("supplier_tax_id_source") or supplier_afm_source,
                    "raw_cache_status": status,
                })
            except Exception as exc:
                failures.append({
                    "ADA": ada,
                    "URL": row.get("url") or DETAIL_URL_TEMPLATE.format(ada=quote(ada, safe="")),
                    "month": f"{int(row.get('year')):04d}-{int(row.get('month')):02d}",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "attempt_count": attempts or args.max_retries + 1,
                    "last_attempted_at": now_iso(),
                })
    return pd.DataFrame(hydrated), pd.DataFrame(failures, columns=FAILED_COLUMNS)


def confidence_rating(*, pagination_complete: bool, exact_500_flag: bool, hydration_pct: float,
                      amount_pct: float, supplier_pct: float) -> str:
    if exact_500_flag or not pagination_complete or hydration_pct < 20:
        return "red"
    if hydration_pct < 70 or amount_pct < 35 or supplier_pct < 35:
        return "yellow"
    return "green"


def build_monthly_coverage(index_df: pd.DataFrame, hydrated_df: pd.DataFrame,
                           failures_df: pd.DataFrame, audits: list[MonthAudit], org: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    audit_by_month = {(a.year, a.month): a for a in audits}
    all_months = sorted(set(audit_by_month) | set(zip(index_df.get("year", []), index_df.get("month", []))))
    for year, month in all_months:
        month_index = index_df[(index_df["year"] == year) & (index_df["month"] == month)]
        month_hydrated = hydrated_df[(hydrated_df.get("year", pd.Series(dtype=int)) == year) & (hydrated_df.get("month", pd.Series(dtype=int)) == month)] if not hydrated_df.empty else pd.DataFrame()
        failed = failures_df[failures_df["month"] == f"{int(year):04d}-{int(month):02d}"] if not failures_df.empty else pd.DataFrame()
        indexed = len(month_index)
        hydrated_count = len(month_hydrated)
        procurement_rows = sum(1 for item in month_index.to_dict("records") if should_hydrate(item, False))
        amount_count = int(month_hydrated["amount"].notna().sum()) if "amount" in month_hydrated else 0
        supplier_count = int(month_hydrated["supplier_name"].notna().sum()) if "supplier_name" in month_hydrated else 0
        hydration_pct = round((hydrated_count / indexed * 100), 2) if indexed else 0.0
        amount_pct = round((amount_count / hydrated_count * 100), 2) if hydrated_count else 0.0
        supplier_pct = round((supplier_count / hydrated_count * 100), 2) if hydrated_count else 0.0
        audit = audit_by_month.get((int(year), int(month)), MonthAudit(int(year), int(month)))
        rows.append({
            "org_uid": org,
            "year": int(year),
            "month": int(month),
            "indexed_decisions": indexed,
            "hydrated_decisions": hydrated_count,
            "hydration_pct": hydration_pct,
            "procurement_rows": procurement_rows,
            "rows_with_amount": amount_count,
            "amount_extraction_pct": amount_pct,
            "rows_with_supplier": supplier_count,
            "supplier_extraction_pct": supplier_pct,
            "exact_500_flag": audit.exact_500_flag,
            "pagination_complete": audit.pagination_complete,
            "failed_hydrations": len(failed),
            "confidence_rating": confidence_rating(
                pagination_complete=audit.pagination_complete, exact_500_flag=audit.exact_500_flag,
                hydration_pct=hydration_pct, amount_pct=amount_pct, supplier_pct=supplier_pct,
            ),
        })
    return pd.DataFrame(rows, columns=COVERAGE_COLUMNS)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Robust monthly Diavgeia ingestion")
    parser.add_argument("--org", default=LAMIA_ORG_UID)
    parser.add_argument("--from", dest="month_from", type=parse_month, required=True)
    parser.add_argument("--to", dest="month_to", type=parse_month, required=True)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--hydrate-all", action="store_true")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw/diavgeia"))
    parser.add_argument("--index-root", type=Path, default=Path("data/index"))
    parser.add_argument("--quality-root", type=Path, default=Path("data/quality"))
    parser.add_argument("--normalized-root", type=Path, default=Path("data/normalized"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.month_from > args.month_to:
        print("--from must be earlier than or equal to --to", file=sys.stderr)
        return 2
    fetched_at = now_iso()
    index_rows: list[dict[str, Any]] = []
    audits: list[MonthAudit] = []
    with requests.Session() as session:
        for month in iter_months(args.month_from, args.month_to):
            raw_rows, audit = fetch_month_index(session, args.org, month, size=args.page_size,
                                                timeout=args.timeout, max_retries=args.max_retries, sleep=args.sleep)
            audits.append(audit)
            index_rows.extend(index_row(item, args.org, month, fetched_at, audit.source_query) for item in raw_rows)
            print(f"Indexed {len(raw_rows)} decisions for {month:%Y-%m}", file=sys.stderr)
    index_df = pd.DataFrame(index_rows, columns=INDEX_COLUMNS).drop_duplicates(subset=["ada"], keep="first")
    org_index_root = args.index_root / f"org={args.org}"
    write_table(index_df, org_index_root / "decision_index")

    hydrated_df, failures_df = hydrate_index(index_df, args.org, args)
    org_norm_root = args.normalized_root / f"org={args.org}"
    write_table(hydrated_df, org_norm_root / "hydrated_decisions")

    quality_dir = args.quality_root / f"org={args.org}"
    quality_dir.mkdir(parents=True, exist_ok=True)
    failures_df.to_csv(quality_dir / "failed_hydrations.csv", index=False)
    coverage_df = build_monthly_coverage(index_df, hydrated_df, failures_df, audits, args.org)
    coverage_df.to_csv(quality_dir / "monthly_coverage.csv", index=False)
    print(f"Wrote {org_index_root / 'decision_index.csv'}", file=sys.stderr)
    print(f"Wrote {quality_dir / 'monthly_coverage.csv'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
