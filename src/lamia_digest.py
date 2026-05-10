#!/usr/bin/env python3
"""Build a Lamia Municipality-only Diavgeia digest.

This is intentionally separate from the general monthly digest pipeline. It uses
Diavgeia organizationUid 6166 (ΔΗΜΟΣ ΛΑΜΙΕΩΝ) and writes JSON/Markdown outputs
under artifacts/lamia/.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://diavgeia.gov.gr"
EXPORT_URL = f"{BASE_URL}/luminapi/api/search/export"
LAMIA_ORG_UID = "6166"
LAMIA_NAME = "ΔΗΜΟΣ ΛΑΜΙΕΩΝ"
LAMIA_SLUG = "dhmos_lamieon"
DEFAULT_OUTPUT_DIR = Path("artifacts/lamia")

DECISION_TYPE_LABELS = {
    "Α.1": "Regulatory act",
    "Α.2": "Internal regulation",
    "Β.1.1": "Budget commitment",
    "Β.1.2": "Budget amendment",
    "Β.1.3": "Payment warrant",
    "Β.2.1": "Expenditure approval",
    "Β.2.2": "Payment finalization",
    "Γ.2": "Personnel change",
    "Δ.1": "Procurement assignment",
    "Δ.2.2": "Contract award",
    "2.4.7.1": "Other administrative act",
}

MONEY_KEYS = (
    "amount",
    "expenseAmount",
    "netAmount",
    "totalAmount",
    "amountWithVAT",
    "budgetAmount",
)

SIGNER_KEYS = (
    "signer",
    "signerName",
    "finalSigner",
    "finalSignerName",
    "issuerName",
)

UNIT_KEYS = (
    "unit",
    "unitLabel",
    "organizationalUnit",
    "organizationalUnitLabel",
    "organizationUnit",
    "organizationUnitLabel",
)


def previous_month_bounds(today: date | None = None) -> tuple[date, date]:
    """Return the first and last date of the previous calendar month."""
    today = today or date.today()
    first_this_month = today.replace(day=1)
    last_previous_month = first_this_month - timedelta(days=1)
    first_previous_month = last_previous_month.replace(day=1)
    return first_previous_month, last_previous_month


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM-DD, got {value!r}") from exc


def build_query(date_from: date | None, date_to: date | None) -> str:
    parts = [f'organizationUid:"{LAMIA_ORG_UID}"']
    if date_from or date_to:
        left = (date_from or date(1970, 1, 1)).isoformat() + "T00:00:00"
        right = (date_to or date(2099, 12, 31)).isoformat() + "T23:59:59"
        parts.append(f"issueDate:[DT({left}) TO DT({right})]")
    return " AND ".join(parts)


def fetch_export(query: str, limit: int, page_size: int, timeout: int) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    page = 0
    remaining = limit

    with requests.Session() as session:
        while remaining > 0:
            size = min(page_size, remaining)
            params = {
                "q": query,
                "sort": "recent",
                "wt": "json",
                "page": page,
                "size": size,
            }
            response = session.get(EXPORT_URL, params=params, timeout=timeout)
            response.raise_for_status()
            batch = extract_decisions(response.json())
            if not batch:
                break
            decisions.extend(batch[:remaining])
            remaining = limit - len(decisions)
            if len(batch) < size:
                break
            page += 1

    return decisions


def extract_decisions(payload: Any) -> list[dict[str, Any]]:
    """Accept common Diavgeia JSON response shapes."""
    if not isinstance(payload, dict):
        return []

    for key in ("decisionResultList", "decisions", "diavgeia_decisions"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    decision_results = payload.get("decisionresults") or payload.get("decisionResults")
    if isinstance(decision_results, dict):
        value = decision_results.get("decision") or decision_results.get("decisions") or []
        if isinstance(value, dict):
            return [value]
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    return []


def first_present(source: dict[str, Any], keys: tuple[str, ...] | list[str]) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, "", []):
            return value
    return None


def normalize_date(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:19], fmt).date().isoformat()
        except ValueError:
            continue
    return text


def normalize_amount(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    # Preserve unparseable strings rather than hiding available source data.
    normalized = text.replace("€", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return text


def decision_url(ada: str | None, hit: dict[str, Any]) -> str | None:
    for key in ("url", "documentUrl"):
        value = hit.get(key)
        if value:
            return str(value)
    if ada:
        return f"{BASE_URL}/decision/view/{ada}"
    return None


def categorize(hit: dict[str, Any], decision_type: str | None, decision_type_label: str | None) -> str:
    if decision_type_label:
        return decision_type_label
    if decision_type and decision_type in DECISION_TYPE_LABELS:
        return DECISION_TYPE_LABELS[decision_type]

    title = str(hit.get("subject") or hit.get("title") or "").lower()
    if re.search(r"(σύμβαση|ανάθεση|προμήθεια|contract|procurement)", title):
        return "Procurement / contract"
    if re.search(r"(πληρωμή|δαπάνη|ένταλμα|πίστωση|payment|expense)", title):
        return "Finance / payment"
    if re.search(r"(προσωπικ|υπάλληλ|διορισμ|personnel|staff)", title):
        return "Personnel"
    if re.search(r"(άδεια|license|permit)", title):
        return "Permit / license"
    return "Other"


def normalize_decision(hit: dict[str, Any]) -> dict[str, Any]:
    ada = first_present(hit, ("ada", "ADA"))
    decision_type = first_present(hit, ("decisionTypeUid", "decisionTypeId", "type"))
    decision_type_label = first_present(hit, ("decisionTypeLabel", "typeLabel"))
    signer = first_present(hit, SIGNER_KEYS)
    unit = first_present(hit, UNIT_KEYS)
    amount = normalize_amount(first_present(hit, MONEY_KEYS))

    normalized = {
        "title": first_present(hit, ("subject", "title")),
        "decision_date": normalize_date(first_present(hit, ("issueDate", "decisionDate"))),
        "ada": ada,
        "decision_type": decision_type,
        "decision_type_label": decision_type_label,
        "signer": signer,
        "unit": unit,
        "amount": amount,
        "url": decision_url(ada, hit),
        "category": categorize(hit, decision_type, decision_type_label),
    }
    return normalized


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def markdown_cell(value: Any) -> str:
    if value in (None, ""):
        return "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    metadata = payload["metadata"]
    decisions = payload["decisions"]
    lines = [
        "# Lamia Municipality Diavgeia Digest",
        "",
        f"- Organization: {metadata['organization_name']} (`{metadata['organization_uid']}`)",
        f"- Slug: `{metadata['organization_slug']}`",
        f"- Period: {metadata['date_from']} → {metadata['date_to']}",
        f"- Decisions: {metadata['count']}",
        f"- Query: `{metadata['query']}`",
        "",
    ]

    if not decisions:
        lines.extend(["No decisions found for this period.", ""])
    else:
        lines.extend(
            [
                "| Date | ΑΔΑ | Category | Type | Title | Signer / Unit | Amount | URL |",
                "| --- | --- | --- | --- | --- | --- | ---: | --- |",
            ]
        )
        for item in decisions:
            signer_unit = " / ".join(part for part in (item.get("signer"), item.get("unit")) if part) or None
            url = item.get("url")
            ada = item.get("ada")
            ada_text = f"[{ada}]({url})" if ada and url else markdown_cell(ada)
            lines.append(
                "| "
                + " | ".join(
                    [
                        markdown_cell(item.get("decision_date")),
                        ada_text,
                        markdown_cell(item.get("category")),
                        markdown_cell(item.get("decision_type_label") or item.get("decision_type")),
                        markdown_cell(item.get("title")),
                        markdown_cell(signer_unit),
                        markdown_cell(item.get("amount")),
                        markdown_cell(url),
                    ]
                )
                + " |"
            )
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_from, default_to = previous_month_bounds()
    parser = argparse.ArgumentParser(description="Build a Lamia Municipality Diavgeia digest")
    parser.add_argument("--from", dest="date_from", type=parse_iso_date, default=default_from, help="Start date (YYYY-MM-DD). Defaults to the first day of the previous month.")
    parser.add_argument("--to", dest="date_to", type=parse_iso_date, default=default_to, help="End date (YYYY-MM-DD). Defaults to the last day of the previous month.")
    parser.add_argument("--limit", type=int, default=int(os.getenv("LAMIA_DIGEST_LIMIT", "500")), help="Maximum decisions to fetch.")
    parser.add_argument("--page-size", type=int, default=200, help="Diavgeia page size for export requests.")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for Lamia artifacts.")
    parser.add_argument("--verbose", action="store_true", help="Print query and output paths.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.date_from > args.date_to:
        print("--from must be earlier than or equal to --to", file=sys.stderr)
        return 2
    if args.limit < 1:
        print("--limit must be at least 1", file=sys.stderr)
        return 2

    query = build_query(args.date_from, args.date_to)
    if args.verbose:
        print(f"Query: {query}", file=sys.stderr)

    try:
        raw_decisions = fetch_export(query, limit=args.limit, page_size=args.page_size, timeout=args.timeout)
    except requests.RequestException as exc:
        print(f"Failed to fetch Lamia decisions from Diavgeia: {exc}", file=sys.stderr)
        return 1

    decisions = [normalize_decision(item) for item in raw_decisions]

    payload = {
        "metadata": {
            "organization_name": LAMIA_NAME,
            "organization_slug": LAMIA_SLUG,
            "organization_uid": LAMIA_ORG_UID,
            "date_from": args.date_from.isoformat(),
            "date_to": args.date_to.isoformat(),
            "query": query,
            "count": len(decisions),
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "source": EXPORT_URL,
        },
        "decisions": decisions,
    }

    json_path = args.output_dir / "lamia_digest.json"
    md_path = args.output_dir / "lamia_digest.md"
    write_json(json_path, payload)
    write_markdown(md_path, payload)

    print(f"Wrote {len(decisions)} Lamia decisions to {json_path} and {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
