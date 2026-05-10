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
import time
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

BASE_URL = "https://diavgeia.gov.gr"
EXPORT_URL = f"{BASE_URL}/luminapi/api/search/export"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/opendata/decisions/{{ada}}"
ORG_METADATA_URL_TEMPLATE = f"{BASE_URL}/luminapi/opendata/organizations/{{organization_id}}/{{kind}}"
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
    "paymentAmount",
    "expenseAmount",
    "netAmount",
    "totalAmount",
    "amountWithVAT",
    "budgetAmount",
)

AMOUNT_LABEL_TOKENS = (
    "ποσο",
    "amount",
    "paymentamount",
    "expenseamount",
)

EXTRA_FIELD_VALUE_KEYS = (
    "value",
    "fieldValue",
    "field_value",
    "values",
    "amount",
)

EXTRA_FIELD_LABEL_KEYS = (
    "label",
    "name",
    "field",
    "fieldName",
    "fieldLabel",
    "field_label",
    "key",
    "title",
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

BUDGET_SOURCE_KEYS = (
    "budgettype",
    "budgetType",
    "budget_source",
    "budgetSource",
    "fundingSource",
    "financingSource",
)

SIGNER_ID_KEYS = ("signerIds", "signerId", "signers")
UNIT_ID_KEYS = ("unitIds", "unitId", "units")
ORG_ID_KEYS = ("organizationId", "organizationUid", "orgId")


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


def normalize_amount(value: Any) -> int | float | None:
    """Return a numeric amount only; non-numeric labels are not amounts."""
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (dict, list)):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if value == value else None

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("€", "").replace(" ", "")
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    elif re.fullmatch(r"[+-]?\d{1,3}(?:\.\d{3})+", normalized):
        normalized = normalized.replace(".", "")

    try:
        return float(normalized)
    except ValueError:
        return None


def has_amount(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def normalize_label(value: Any) -> str:
    text = str(value).strip().lower()
    decomposed = unicodedata.normalize("NFD", text)
    without_accents = "".join(char for char in decomposed if unicodedata.category(char) != "Mn")
    return re.sub(r"[\s_\-]+", "", without_accents)


def is_amount_label(value: Any) -> bool:
    if value in (None, ""):
        return False
    label = normalize_label(value)
    return any(token in label for token in AMOUNT_LABEL_TOKENS)


def is_budget_source_label(value: Any) -> bool:
    if value in (None, ""):
        return False
    label = normalize_label(value)
    return any(normalize_label(token) == label for token in BUDGET_SOURCE_KEYS)


def normalize_text(value: Any) -> str | None:
    if value in (None, "", []):
        return None
    if isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


def extract_budget_source(source: Any) -> str | None:
    if isinstance(source, dict):
        for key, value in source.items():
            key_text = str(key)
            if is_budget_source_label(key_text):
                text = normalize_text(value)
                if text:
                    return text
            if isinstance(value, dict):
                labels = [value.get(label_key) for label_key in EXTRA_FIELD_LABEL_KEYS if value.get(label_key) not in (None, "", [])]
                if any(is_budget_source_label(label) for label in labels):
                    for value_key in EXTRA_FIELD_VALUE_KEYS:
                        text = normalize_text(value.get(value_key))
                        if text:
                            return text
                nested = extract_budget_source(value)
                if nested:
                    return nested
            elif isinstance(value, list):
                nested = extract_budget_source(value)
                if nested:
                    return nested
    elif isinstance(source, list):
        for item in source:
            nested = extract_budget_source(item)
            if nested:
                return nested
    return None


def format_amount(value: Any) -> str:
    if not has_amount(value):
        return "—"
    return f"{value:g}" if isinstance(value, float) else str(value)


def amount_path(path: tuple[str, ...], key: str) -> str:
    return ".".join((*path, key)) if path else key


def amount_from_value(value: Any, path: tuple[str, ...]) -> tuple[Any, str | None]:
    if isinstance(value, dict):
        for key in EXTRA_FIELD_VALUE_KEYS:
            if key in value:
                amount, source = amount_from_value(value.get(key), (*path, key))
                if has_amount(amount):
                    return amount, source
        return extract_amount(value, path)
    if isinstance(value, list):
        for index, item in enumerate(value):
            amount, source = amount_from_value(item, (*path, str(index)))
            if has_amount(amount):
                return amount, source
        return None, None

    amount = normalize_amount(value)
    if has_amount(amount):
        return amount, ".".join(path) if path else None
    return None, None


def extract_extra_field_amount(value: Any, path: tuple[str, ...]) -> tuple[Any, str | None]:
    if isinstance(value, dict):
        for key, item in value.items():
            if is_amount_label(key):
                amount, source = amount_from_value(item, (*path, str(key)))
                if has_amount(amount):
                    return amount, source or amount_path(path, str(key))

        labels = [value.get(key) for key in EXTRA_FIELD_LABEL_KEYS if value.get(key) not in (None, "", [])]
        if any(is_amount_label(label) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                amount, source = amount_from_value(value.get(key), (*path, key))
                if has_amount(amount):
                    return amount, source or amount_path(path, key)

    if isinstance(value, list):
        for index, item in enumerate(value):
            amount, source = extract_extra_field_amount(item, (*path, str(index)))
            if has_amount(amount):
                return amount, source

    return None, None


def extract_amount(source: Any, path: tuple[str, ...] = ()) -> tuple[Any, str | None]:
    """Find the first financial amount in known top-level and nested shapes."""
    if isinstance(source, dict):
        extra_fields = source.get("extraFieldValues")
        if extra_fields not in (None, "", []):
            amount, amount_source = extract_extra_field_amount(extra_fields, (*path, "extraFieldValues"))
            if has_amount(amount):
                return amount, amount_source

        for key, value in source.items():
            key_text = str(key)
            if key in MONEY_KEYS or is_amount_label(key_text):
                amount, amount_source = amount_from_value(value, (*path, key_text))
                if has_amount(amount):
                    return amount, amount_source or amount_path(path, key_text)

        for key, value in source.items():
            if isinstance(value, (dict, list)):
                amount, amount_source = extract_amount(value, (*path, str(key)))
                if has_amount(amount):
                    return amount, amount_source

    elif isinstance(source, list):
        for index, item in enumerate(source):
            amount, amount_source = extract_amount(item, (*path, str(index)))
            if has_amount(amount):
                return amount, amount_source

    return None, None


def detail_url(ada: str) -> str:
    return DETAIL_URL_TEMPLATE.format(ada=quote(ada, safe=""))


def fetch_full_decision(
    session: requests.Session,
    ada: str,
    timeout: float,
    retries: int = 2,
) -> dict[str, Any]:
    last_error: requests.RequestException | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(detail_url(ada), timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            raise requests.RequestException(f"Unexpected detail payload type: {type(payload).__name__}")
        except (ValueError, requests.RequestException) as exc:
            if isinstance(exc, requests.RequestException):
                last_error = exc
            else:
                last_error = requests.RequestException(str(exc))
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))

    assert last_error is not None
    raise last_error


def unwrap_decision_detail(payload: dict[str, Any]) -> Any:
    for key in ("decision", "decisionResult", "data"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def values_as_list(value: Any) -> list[Any]:
    if value in (None, "", []):
        return []
    if isinstance(value, list):
        return value
    return [value]


def extract_ids(source: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    ids: list[str] = []
    for key in keys:
        for item in values_as_list(source.get(key)):
            if isinstance(item, dict):
                value = first_present(item, ("uid", "id", "signerId", "unitId"))
            else:
                value = item
            if value not in (None, "", []):
                ids.append(str(value))
        if ids:
            break
    return ids


def metadata_item_name(item: Any) -> tuple[str | None, str | None]:
    if not isinstance(item, dict):
        return None, None
    item_id = first_present(item, ("uid", "id", "signerId", "unitId"))
    name = first_present(item, ("label", "name", "description", "title"))
    if not name:
        first_name = first_present(item, ("firstName", "firstname"))
        last_name = first_present(item, ("lastName", "lastname", "surname"))
        name = " ".join(str(part).strip() for part in (first_name, last_name) if part not in (None, "", [])) or None
    return (str(item_id) if item_id not in (None, "", []) else None, str(name) if name else None)


def extract_metadata_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("signers", "units", "items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [payload]
    return []


def fetch_org_lookup(
    session: requests.Session,
    organization_id: str | None,
    kind: str,
    timeout: float,
    cache: dict[tuple[str, str], dict[str, str]],
) -> dict[str, str]:
    if not organization_id:
        return {}
    cache_key = (organization_id, kind)
    if cache_key in cache:
        return cache[cache_key]

    url = ORG_METADATA_URL_TEMPLATE.format(organization_id=quote(organization_id, safe=""), kind=kind)
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except (ValueError, requests.RequestException) as exc:
        print(f"Warning: failed to fetch {kind} metadata for organization {organization_id}: {exc}", file=sys.stderr)
        cache[cache_key] = {}
        return {}

    lookup: dict[str, str] = {}
    for item in extract_metadata_items(payload):
        item_id, name = metadata_item_name(item)
        if item_id and name:
            lookup[item_id] = name
    cache[cache_key] = lookup
    return lookup


def resolve_names(ids: list[str], lookup: dict[str, str]) -> str | None:
    names = [lookup[item_id] for item_id in ids if item_id in lookup and lookup[item_id]]
    return ", ".join(names) if names else None


def apply_detail_enrichment(
    item: dict[str, Any],
    detail: Any,
    session: requests.Session,
    timeout: float,
    metadata_cache: dict[tuple[str, str], dict[str, str]],
) -> None:
    if not isinstance(detail, dict):
        return

    amount, source = extract_amount(detail)
    if has_amount(amount) and not has_amount(item.get("amount")):
        item["amount"] = amount
        item["amount_source"] = f"full_record:{source}" if source else "full_record"

    budget_source = extract_budget_source(detail)
    if budget_source and not item.get("budget_source"):
        item["budget_source"] = budget_source

    organization_id = first_present(detail, ORG_ID_KEYS) or item.get("organization_id")
    organization_id = str(organization_id) if organization_id not in (None, "", []) else None

    signer_ids = item.get("signer_ids") or extract_ids(detail, SIGNER_ID_KEYS)
    unit_ids = item.get("unit_ids") or extract_ids(detail, UNIT_ID_KEYS)
    item["signer_ids"] = signer_ids
    item["unit_ids"] = unit_ids

    signer = first_present(detail, SIGNER_KEYS)
    unit = first_present(detail, UNIT_KEYS)
    if not signer and signer_ids:
        signer = resolve_names(signer_ids, fetch_org_lookup(session, organization_id, "signers", timeout, metadata_cache))
    if not unit and unit_ids:
        unit = resolve_names(unit_ids, fetch_org_lookup(session, organization_id, "units", timeout, metadata_cache))

    if signer and not item.get("signer"):
        item["signer"] = signer
    if unit and not item.get("unit"):
        item["unit"] = unit


def enrich_missing_amounts(
    decisions: list[dict[str, Any]],
    *,
    enabled: bool,
    timeout: float,
    max_fetches: int | None,
) -> dict[str, int]:
    summary = {
        "decisions_fetched": len(decisions),
        "amounts_found_before_enrichment": sum(1 for item in decisions if has_amount(item.get("amount"))),
        "signers_found_before_enrichment": sum(1 for item in decisions if item.get("signer")),
        "units_found_before_enrichment": sum(1 for item in decisions if item.get("unit")),
        "details_fetched": 0,
        "amounts_found_after_enrichment": 0,
        "signers_found_after_enrichment": 0,
        "units_found_after_enrichment": 0,
        "detail_fetch_failures": 0,
    }

    if not enabled:
        summary["amounts_found_after_enrichment"] = summary["amounts_found_before_enrichment"]
        summary["signers_found_after_enrichment"] = summary["signers_found_before_enrichment"]
        summary["units_found_after_enrichment"] = summary["units_found_before_enrichment"]
        return summary

    metadata_cache: dict[tuple[str, str], dict[str, str]] = {}
    with requests.Session() as session:
        for item in decisions:
            needs_amount = not has_amount(item.get("amount"))
            needs_signer = not item.get("signer")
            needs_unit = not item.get("unit")
            if not (needs_amount or needs_signer or needs_unit):
                continue

            detail_attempts = summary["details_fetched"] + summary["detail_fetch_failures"]
            if max_fetches is not None and detail_attempts >= max_fetches:
                break

            ada = item.get("ada")
            if not ada:
                continue

            try:
                payload = fetch_full_decision(session, str(ada), timeout=timeout)
            except requests.RequestException as exc:
                summary["detail_fetch_failures"] += 1
                print(f"Warning: failed to fetch full record for ADA {ada}: {exc}", file=sys.stderr)
                continue

            summary["details_fetched"] += 1
            item["enriched_from_full_record"] = True
            apply_detail_enrichment(item, unwrap_decision_detail(payload), session, timeout, metadata_cache)

    summary["amounts_found_after_enrichment"] = sum(1 for item in decisions if has_amount(item.get("amount")))
    summary["signers_found_after_enrichment"] = sum(1 for item in decisions if item.get("signer"))
    summary["units_found_after_enrichment"] = sum(1 for item in decisions if item.get("unit"))
    return summary


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
    amount, amount_source = extract_amount(hit)
    budget_source = extract_budget_source(hit)
    organization_id = first_present(hit, ORG_ID_KEYS)

    normalized = {
        "title": first_present(hit, ("subject", "title")),
        "decision_date": normalize_date(first_present(hit, ("issueDate", "decisionDate"))),
        "ada": ada,
        "decision_type": decision_type,
        "decision_type_label": decision_type_label,
        "signer": signer,
        "unit": unit,
        "signer_ids": extract_ids(hit, SIGNER_ID_KEYS),
        "unit_ids": extract_ids(hit, UNIT_ID_KEYS),
        "organization_id": organization_id,
        "amount": amount,
        "amount_source": f"export:{amount_source}" if amount_source else None,
        "budget_source": budget_source,
        "enriched_from_full_record": False,
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
                "| Date | ΑΔΑ | Category | Type | Title | Signer / Unit | Amount | Budget source | URL |",
                "| --- | --- | --- | --- | --- | --- | ---: | --- | --- |",
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
                        format_amount(item.get("amount")),
                        markdown_cell(item.get("budget_source")),
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
    parser.add_argument("--detail-timeout", type=float, default=20.0, help="HTTP timeout in seconds for full-record enrichment requests.")
    parser.add_argument("--max-detail-fetches", type=int, default=None, help="Optional cap on full-record enrichment attempts for decisions missing amounts.")
    enrich_group = parser.add_mutually_exclusive_group()
    enrich_group.add_argument("--enrich-details", dest="enrich_details", action="store_true", default=True, help="Fetch full decision records when export records do not include an amount (default).")
    enrich_group.add_argument("--no-enrich-details", dest="enrich_details", action="store_false", help="Disable full-record enrichment for missing amounts.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for Lamia artifacts.")
    parser.add_argument("--verbose", action="store_true", help="Print query, enrichment summary, and output paths.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.date_from > args.date_to:
        print("--from must be earlier than or equal to --to", file=sys.stderr)
        return 2
    if args.limit < 1:
        print("--limit must be at least 1", file=sys.stderr)
        return 2
    if args.detail_timeout <= 0:
        print("--detail-timeout must be greater than 0", file=sys.stderr)
        return 2
    if args.max_detail_fetches is not None and args.max_detail_fetches < 0:
        print("--max-detail-fetches must be zero or greater", file=sys.stderr)
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
    enrichment_summary = enrich_missing_amounts(
        decisions,
        enabled=args.enrich_details,
        timeout=args.detail_timeout,
        max_fetches=args.max_detail_fetches,
    )

    if args.verbose:
        print("Enrichment summary:", file=sys.stderr)
        for key, value in enrichment_summary.items():
            print(f"  {key}: {value}", file=sys.stderr)

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
            "detail_source": DETAIL_URL_TEMPLATE,
            "enrich_details": args.enrich_details,
            "enrichment_summary": enrichment_summary,
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
