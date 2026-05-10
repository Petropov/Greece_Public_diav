#!/usr/bin/env python3
"""Build a Lamia Municipality-only Diavgeia digest.

This is intentionally separate from the general monthly digest pipeline. It uses
Diavgeia organizationUid 6166 (ΔΗΜΟΣ ΛΑΜΙΕΩΝ) and writes JSON/Markdown outputs
under artifacts/lamia/.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import unicodedata
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

BASE_URL = "https://diavgeia.gov.gr"
EXPORT_URL = f"{BASE_URL}/luminapi/api/search/export"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/opendata/decisions/{{ada}}"
ORG_METADATA_URL_TEMPLATE = (
    f"{BASE_URL}/luminapi/opendata/organizations/{{organization_id}}/{{kind}}"
)
LAMIA_ORG_UID = "6166"
LAMIA_NAME = "ΔΗΜΟΣ ΛΑΜΙΕΩΝ"
LAMIA_SLUG = "dhmos_lamieon"
DEFAULT_OUTPUT_DIR = Path("artifacts/lamia")

# Known Diavgeia decision-type identifiers seen in Lamia exports.  The raw
# identifier is kept in each output row while these labels make digest tables
# readable for humans.
DECISION_TYPE_LABELS = {
    "Α.1": "Regulatory act",
    "Α.2": "Internal regulation",
    "Β.1.1": "Budget commitment / obligation approval",
    "Β.1.2": "Budget amendment",
    "Β.1.3": "Payment warrant",
    "Β.2.1": "Payment / expenditure approval",
    "Β.2.2": "Payment / expenditure approval revocation or correction",
    "Γ.2": "Personnel change",
    "Δ.1": "Direct procurement assignment",
    "Δ.2.1": "Open procurement notice",
    "Δ.2.2": "Contract award",
    "Δ.2.3": "Contract signing / agreement",
    "Δ.2.4": "Procurement cancellation / revocation",
    "2.4.7.1": "Other administrative act",
}

SUBJECT_KEYS = (
    "subject",
    "title",
    "summary",
    "description",
    "decisionSubject",
    "documentSubject",
)

ISSUE_DATE_KEYS = (
    "issueDate",
    "decisionDate",
    "issue_date",
    "decision_date",
    "date",
    "publishDate",
    "publishedDate",
)

PROTOCOL_NUMBER_KEYS = (
    "protocolNumber",
    "protocolNo",
    "protocol_number",
    "protocol",
    "documentProtocolNumber",
)

DECISION_TYPE_KEYS = (
    "decisionTypeUid",
    "decisionTypeId",
    "decision_type_raw",
    "decision_type_id",
    "decisionType",
    "type",
)

DECISION_TYPE_LABEL_KEYS = (
    "decisionTypeLabel",
    "decision_type",
    "decision_type_label",
    "typeLabel",
)

REQUIRED_DECISION_FIELDS = ("ada", "issue_date", "subject", "decision_type")

PROCUREMENT_TOKENS = (
    "σύμβαση",
    "συμβαση",
    "ανάθεση",
    "αναθεση",
    "προμήθεια",
    "προμηθεια",
    "δαπάνη",
    "δαπανη",
    "contract",
    "procurement",
    "award",
    "supplier",
)

TOP_PROCUREMENTS_LIMIT = 10
DUPLICATE_SIMILARITY_THRESHOLD = 0.82

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


SUPPLIER_NAME_KEYS = (
    "supplier_name",
    "supplierName",
    "supplier",
    "vendor_name",
    "vendorName",
    "vendor",
    "contractor_name",
    "contractorName",
    "contractor",
    "counterparty_name",
    "counterpartyName",
    "counterparty",
    "beneficiary_name",
    "beneficiaryName",
    "beneficiary",
    "recipient_name",
    "recipientName",
    "recipient",
    "payee_name",
    "payeeName",
    "payee",
    "sponsor_name",
    "sponsorName",
    "sponsor",
    "contractorTitle",
    "companyName",
)

SUPPLIER_TAX_ID_KEYS = (
    "supplier_tax_id",
    "supplierTaxId",
    "supplierAfm",
    "supplierAFM",
    "vendor_tax_id",
    "vendorTaxId",
    "vendorAfm",
    "vendorAFM",
    "contractor_tax_id",
    "contractorTaxId",
    "contractorAfm",
    "contractorAFM",
    "counterparty_tax_id",
    "counterpartyTaxId",
    "counterpartyAfm",
    "counterpartyAFM",
    "beneficiary_tax_id",
    "beneficiaryTaxId",
    "beneficiaryAfm",
    "beneficiaryAFM",
    "recipient_tax_id",
    "recipientTaxId",
    "payee_tax_id",
    "payeeTaxId",
    "sponsor_afm",
    "sponsorAFM",
    "sponsorTaxId",
    "afm",
    "AFM",
    "vatNumber",
    "vatId",
    "taxId",
    "taxNumber",
    "tin",
)

SUPPLIER_NAME_LABEL_TOKENS = (
    "supplier",
    "vendor",
    "contractor",
    "counterparty",
    "beneficiary",
    "recipient",
    "payee",
    "sponsor",
    "αναδοχος",
    "αναδοχου",
    "προμηθευτης",
    "προμηθευτη",
    "προμηθευτησ",
    "αντισυμβαλλομενος",
    "δικαιουχος",
    "δικαιουχου",
    "αποδεκτης",
    "αποδεκτη",
    "επωνυμια",
)

SUPPLIER_TAX_ID_LABEL_TOKENS = (
    "afm",
    "αφμ",
    "taxid",
    "taxnumber",
    "vatnumber",
    "vatid",
    "tin",
)

TOP_SUPPLIERS_LIMIT = 10

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


def fetch_export(
    query: str, limit: int, page_size: int, timeout: int
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    seen_adas: set[str] = set()
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

            new_rows: list[dict[str, Any]] = []
            for item in batch:
                ada = normalize_text(first_present(item, ("ada", "ADA")))
                if ada:
                    if ada in seen_adas:
                        continue
                    seen_adas.add(ada)
                new_rows.append(item)

            if not new_rows:
                break

            decisions.extend(new_rows[:remaining])
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
        value = (
            decision_results.get("decision") or decision_results.get("decisions") or []
        )
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
    without_accents = "".join(
        char for char in decomposed if unicodedata.category(char) != "Mn"
    )
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


def is_supplier_tax_id_label(value: Any) -> bool:
    if value in (None, ""):
        return False
    label = normalize_label(value)
    if "amountwithvat" in label:
        return False
    return any(token in label for token in SUPPLIER_TAX_ID_LABEL_TOKENS)


def is_supplier_name_label(value: Any) -> bool:
    if value in (None, ""):
        return False
    if is_supplier_tax_id_label(value):
        return False
    label = normalize_label(value)
    return any(normalize_label(token) in label for token in SUPPLIER_NAME_LABEL_TOKENS)


def normalize_tax_id(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    compact = re.sub(r"[^0-9A-Za-zΑ-Ωα-ω]", "", text)
    digits = re.sub(r"\D", "", compact)
    if len(digits) == 9:
        return digits
    if digits and 7 <= len(digits) <= 15 and len(digits) >= len(compact) - 2:
        return digits
    return compact or None


def value_text_from_extra(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in EXTRA_FIELD_VALUE_KEYS:
            text = normalize_text(value.get(key))
            if text:
                return text
        return None
    if isinstance(value, list):
        texts = [normalize_text(item) for item in value]
        texts = [text for text in texts if text]
        return ", ".join(texts) if texts else None
    return normalize_text(value)


def extract_supplier_fields(
    source: Any, path: tuple[str, ...] = (), supplier_context: bool = False
) -> tuple[str | None, str | None, str | None, str | None]:
    """Find supplier/counterparty name and Greek tax id in raw Diavgeia shapes."""
    supplier_name = None
    supplier_tax_id = None
    name_source = None
    tax_source = None

    def remember_name(value: Any, source_path: tuple[str, ...]) -> None:
        nonlocal supplier_name, name_source
        if supplier_name:
            return
        text = value_text_from_extra(value)
        if text:
            supplier_name = text
            name_source = ".".join(source_path)

    def remember_tax_id(value: Any, source_path: tuple[str, ...]) -> None:
        nonlocal supplier_tax_id, tax_source
        if supplier_tax_id:
            return
        if isinstance(value, dict):
            for key in EXTRA_FIELD_VALUE_KEYS:
                tax = normalize_tax_id(value.get(key))
                if tax:
                    supplier_tax_id = tax
                    tax_source = ".".join((*source_path, key))
                    return
        else:
            tax = normalize_tax_id(value)
            if tax:
                supplier_tax_id = tax
                tax_source = ".".join(source_path)

    if isinstance(source, dict):
        labels = [
            source.get(key)
            for key in EXTRA_FIELD_LABEL_KEYS
            if source.get(key) not in (None, "", [])
        ]
        if any(is_supplier_tax_id_label(label) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                remember_tax_id(source.get(key), (*path, key))
                if supplier_tax_id:
                    break
        if any(is_supplier_name_label(label) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                remember_name(source.get(key), (*path, key))
                if supplier_name:
                    break

        current_context = supplier_context or any(
            is_supplier_name_label(label) for label in labels
        )
        for key, value in source.items():
            key_text = str(key)
            key_path = (*path, key_text)
            if key in SUPPLIER_TAX_ID_KEYS or is_supplier_tax_id_label(key_text):
                remember_tax_id(value, key_path)
            elif key in SUPPLIER_NAME_KEYS or is_supplier_name_label(key_text):
                remember_name(value, key_path)
            elif current_context and normalize_label(key_text) in {
                "name",
                "label",
                "title",
                "description",
                "fullname",
                "επωνυμια",
                "ονομα",
            }:
                remember_name(value, key_path)

            if supplier_name and supplier_tax_id:
                return supplier_name, supplier_tax_id, name_source, tax_source

        for key, value in source.items():
            if isinstance(value, (dict, list)):
                nested_context = current_context or is_supplier_name_label(key)
                name, tax_id, nested_name_source, nested_tax_source = (
                    extract_supplier_fields(value, (*path, str(key)), nested_context)
                )
                if name and not supplier_name:
                    supplier_name = name
                    name_source = nested_name_source
                if tax_id and not supplier_tax_id:
                    supplier_tax_id = tax_id
                    tax_source = nested_tax_source
                if supplier_name and supplier_tax_id:
                    break

    elif isinstance(source, list):
        for index, item in enumerate(source):
            name, tax_id, nested_name_source, nested_tax_source = (
                extract_supplier_fields(item, (*path, str(index)), supplier_context)
            )
            if name and not supplier_name:
                supplier_name = name
                name_source = nested_name_source
            if tax_id and not supplier_tax_id:
                supplier_tax_id = tax_id
                tax_source = nested_tax_source
            if supplier_name and supplier_tax_id:
                break

    return supplier_name, supplier_tax_id, name_source, tax_source


def first_subject(source: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return the best subject/title-like value and where it came from."""
    for key in SUBJECT_KEYS:
        text = normalize_text(source.get(key))
        if text:
            return text, key

    extra_fields = source.get("extraFieldValues")
    if isinstance(extra_fields, dict):
        for key, value in extra_fields.items():
            if normalize_label(key) in {
                "subject",
                "title",
                "description",
                "summary",
                "thema",
            }:
                text = normalize_text(value)
                if text:
                    return text, f"extraFieldValues.{key}"
    return None, None


def normalized_decision_type(raw_type: Any, label: Any = None) -> str | None:
    label_text = normalize_text(label)
    if label_text:
        return label_text
    raw_text = normalize_text(raw_type)
    if not raw_text:
        return None
    return DECISION_TYPE_LABELS.get(raw_text, raw_text)


def canonical_text(value: Any) -> str:
    if value in (None, "", []):
        return ""
    text = unicodedata.normalize("NFD", str(value).lower())
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^0-9a-zα-ω]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def compact_text(value: Any, *, limit_words: int = 12) -> str:
    words = canonical_text(value).split()
    return "-".join(words[:limit_words]) or "untitled"


def amount_bucket(value: Any) -> str:
    if not has_amount(value):
        return "noamount"
    return str(int(round(float(value) * 100)))


def stable_id(*parts: Any, prefix: str = "lamia") -> str:
    raw = "|".join(str(part) for part in parts if part not in (None, "", []))
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{digest}"


def title_similarity(left: Any, right: Any) -> float:
    left_text = canonical_text(left)
    right_text = canonical_text(right)
    if not left_text or not right_text:
        return 0.0
    return SequenceMatcher(None, left_text, right_text).ratio()


def is_procurement_decision(item: dict[str, Any]) -> bool:
    text = canonical_text(
        " ".join(
            str(part)
            for part in (
                item.get("title"),
                item.get("category"),
                item.get("decision_type"),
                item.get("decision_type_raw"),
            )
            if part
        )
    )
    raw_type = item.get("decision_type_raw")
    if raw_type and str(raw_type).startswith("Δ."):
        return True
    return any(canonical_text(token) in text for token in PROCUREMENT_TOKENS)


def extract_budget_source(source: Any) -> str | None:
    if isinstance(source, dict):
        for key, value in source.items():
            key_text = str(key)
            if is_budget_source_label(key_text):
                text = normalize_text(value)
                if text:
                    return text
            if isinstance(value, dict):
                labels = [
                    value.get(label_key)
                    for label_key in EXTRA_FIELD_LABEL_KEYS
                    if value.get(label_key) not in (None, "", [])
                ]
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


def extract_extra_field_amount(
    value: Any, path: tuple[str, ...]
) -> tuple[Any, str | None]:
    if isinstance(value, dict):
        for key, item in value.items():
            if is_amount_label(key):
                amount, source = amount_from_value(item, (*path, str(key)))
                if has_amount(amount):
                    return amount, source or amount_path(path, str(key))

        labels = [
            value.get(key)
            for key in EXTRA_FIELD_LABEL_KEYS
            if value.get(key) not in (None, "", [])
        ]
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
            amount, amount_source = extract_extra_field_amount(
                extra_fields, (*path, "extraFieldValues")
            )
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
            raise requests.RequestException(
                f"Unexpected detail payload type: {type(payload).__name__}"
            )
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
        name = (
            " ".join(
                str(part).strip()
                for part in (first_name, last_name)
                if part not in (None, "", [])
            )
            or None
        )
    return (
        str(item_id) if item_id not in (None, "", []) else None,
        str(name) if name else None,
    )


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

    url = ORG_METADATA_URL_TEMPLATE.format(
        organization_id=quote(organization_id, safe=""), kind=kind
    )
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except (ValueError, requests.RequestException) as exc:
        print(
            f"Warning: failed to fetch {kind} metadata for organization {organization_id}: {exc}",
            file=sys.stderr,
        )
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
    names = [
        lookup[item_id] for item_id in ids if item_id in lookup and lookup[item_id]
    ]
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

    raw_extra_fields = detail.get("extraFieldValues")
    if raw_extra_fields not in (None, "", []) and not item.get(
        "raw_detail_extra_fields"
    ):
        item["raw_detail_extra_fields"] = raw_extra_fields
        if not item.get("raw_extra_fields"):
            item["raw_extra_fields"] = raw_extra_fields

    supplier_name, supplier_tax_id, supplier_name_source, supplier_tax_id_source = (
        extract_supplier_fields(detail)
    )
    if supplier_name and not item.get("supplier_name"):
        item["supplier_name"] = supplier_name
        item["supplier_name_source"] = (
            f"full_record:{supplier_name_source}"
            if supplier_name_source
            else "full_record"
        )
    if supplier_tax_id and not item.get("supplier_tax_id"):
        item["supplier_tax_id"] = supplier_tax_id
        item["supplier_tax_id_source"] = (
            f"full_record:{supplier_tax_id_source}"
            if supplier_tax_id_source
            else "full_record"
        )

    budget_source = extract_budget_source(detail)
    if budget_source and not item.get("budget_source"):
        item["budget_source"] = budget_source

    title, title_source = first_subject(detail)
    if title and not item.get("subject"):
        item["subject"] = title
    if title and not item.get("title"):
        item["title"] = title
        item["title_source"] = (
            f"full_record:{title_source}" if title_source else "full_record"
        )
        item["missing_subject"] = False

    detail_issue_date = normalize_date(first_present(detail, ISSUE_DATE_KEYS))
    if detail_issue_date and not item.get("issue_date"):
        item["issue_date"] = detail_issue_date
        item["decision_date"] = detail_issue_date

    detail_protocol_number = normalize_text(first_present(detail, PROTOCOL_NUMBER_KEYS))
    if detail_protocol_number and not item.get("protocol_number"):
        item["protocol_number"] = detail_protocol_number

    detail_type_raw = first_present(detail, DECISION_TYPE_KEYS)
    detail_type_label = first_present(detail, DECISION_TYPE_LABEL_KEYS)
    detail_type = normalized_decision_type(detail_type_raw, detail_type_label)
    if detail_type_raw and not item.get("decision_type_raw"):
        item["decision_type_raw"] = detail_type_raw
    if detail_type and not item.get("decision_type"):
        item["decision_type"] = detail_type
        item["decision_type_label"] = detail_type

    organization_id = first_present(detail, ORG_ID_KEYS) or item.get("organization_id")
    organization_id = (
        str(organization_id) if organization_id not in (None, "", []) else None
    )

    signer_ids = item.get("signer_ids") or extract_ids(detail, SIGNER_ID_KEYS)
    unit_ids = item.get("unit_ids") or extract_ids(detail, UNIT_ID_KEYS)
    item["signer_ids"] = signer_ids
    item["unit_ids"] = unit_ids

    signer = first_present(detail, SIGNER_KEYS)
    unit = first_present(detail, UNIT_KEYS)
    if not signer and signer_ids:
        signer = resolve_names(
            signer_ids,
            fetch_org_lookup(
                session, organization_id, "signers", timeout, metadata_cache
            ),
        )
    if not unit and unit_ids:
        unit = resolve_names(
            unit_ids,
            fetch_org_lookup(
                session, organization_id, "units", timeout, metadata_cache
            ),
        )

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
        "amounts_found_before_enrichment": sum(
            1 for item in decisions if has_amount(item.get("amount"))
        ),
        "signers_found_before_enrichment": sum(
            1 for item in decisions if item.get("signer")
        ),
        "units_found_before_enrichment": sum(
            1 for item in decisions if item.get("unit")
        ),
        "suppliers_found_before_enrichment": sum(
            1
            for item in decisions
            if item.get("supplier_name") or item.get("supplier_tax_id")
        ),
        "subjects_missing_before_enrichment": sum(
            1 for item in decisions if not item.get("title")
        ),
        "details_fetched": 0,
        "amounts_found_after_enrichment": 0,
        "signers_found_after_enrichment": 0,
        "units_found_after_enrichment": 0,
        "suppliers_found_after_enrichment": 0,
        "subjects_missing_after_enrichment": 0,
        "detail_fetch_failures": 0,
    }

    if not enabled:
        summary["amounts_found_after_enrichment"] = summary[
            "amounts_found_before_enrichment"
        ]
        summary["signers_found_after_enrichment"] = summary[
            "signers_found_before_enrichment"
        ]
        summary["units_found_after_enrichment"] = summary[
            "units_found_before_enrichment"
        ]
        summary["suppliers_found_after_enrichment"] = summary[
            "suppliers_found_before_enrichment"
        ]
        summary["subjects_missing_after_enrichment"] = summary[
            "subjects_missing_before_enrichment"
        ]
        return summary

    metadata_cache: dict[tuple[str, str], dict[str, str]] = {}
    with requests.Session() as session:
        for item in decisions:
            needs_amount = not has_amount(item.get("amount"))
            needs_signer = not item.get("signer")
            needs_unit = not item.get("unit")
            needs_title = not item.get("title")
            needs_supplier = not (
                item.get("supplier_name") or item.get("supplier_tax_id")
            )
            if not (
                needs_amount
                or needs_signer
                or needs_unit
                or needs_title
                or needs_supplier
            ):
                continue

            detail_attempts = (
                summary["details_fetched"] + summary["detail_fetch_failures"]
            )
            if max_fetches is not None and detail_attempts >= max_fetches:
                break

            ada = item.get("ada")
            if not ada:
                continue

            try:
                payload = fetch_full_decision(session, str(ada), timeout=timeout)
            except requests.RequestException as exc:
                summary["detail_fetch_failures"] += 1
                print(
                    f"Warning: failed to fetch full record for ADA {ada}: {exc}",
                    file=sys.stderr,
                )
                continue

            summary["details_fetched"] += 1
            item["enriched_from_full_record"] = True
            apply_detail_enrichment(
                item, unwrap_decision_detail(payload), session, timeout, metadata_cache
            )

    summary["amounts_found_after_enrichment"] = sum(
        1 for item in decisions if has_amount(item.get("amount"))
    )
    summary["signers_found_after_enrichment"] = sum(
        1 for item in decisions if item.get("signer")
    )
    summary["units_found_after_enrichment"] = sum(
        1 for item in decisions if item.get("unit")
    )
    summary["suppliers_found_after_enrichment"] = sum(
        1
        for item in decisions
        if item.get("supplier_name") or item.get("supplier_tax_id")
    )
    summary["subjects_missing_after_enrichment"] = sum(
        1 for item in decisions if not item.get("title")
    )
    return summary


def decision_url(ada: str | None, hit: dict[str, Any]) -> str | None:
    for key in ("url", "documentUrl"):
        value = hit.get(key)
        if value:
            return str(value)
    if ada:
        return f"{BASE_URL}/decision/view/{ada}"
    return None


def categorize(
    hit: dict[str, Any], decision_type: str | None, decision_type_label: str | None
) -> str:
    normalized_type = normalized_decision_type(decision_type, decision_type_label)
    if normalized_type:
        return normalized_type

    title, _ = first_subject(hit)
    title_text = str(title or "").lower()
    if re.search(
        r"(σύμβαση|συμβαση|ανάθεση|αναθεση|προμήθεια|προμηθεια|contract|procurement)",
        title_text,
    ):
        return "Procurement / contract"
    if re.search(
        r"(πληρωμή|πληρωμη|δαπάνη|δαπανη|ένταλμα|ενταλμα|πίστωση|πιστωση|payment|expense)",
        title_text,
    ):
        return "Finance / payment"
    if re.search(r"(προσωπικ|υπάλληλ|υπαλληλ|διορισμ|personnel|staff)", title_text):
        return "Personnel"
    if re.search(r"(άδεια|αδεια|license|permit)", title_text):
        return "Permit / license"
    return "Other"


def normalize_decision(hit: dict[str, Any]) -> dict[str, Any]:
    ada = normalize_text(first_present(hit, ("ada", "ADA")))
    decision_type_raw = first_present(hit, DECISION_TYPE_KEYS)
    decision_type_label = first_present(hit, DECISION_TYPE_LABEL_KEYS)
    decision_type = normalized_decision_type(decision_type_raw, decision_type_label)
    signer = first_present(hit, SIGNER_KEYS)
    unit = first_present(hit, UNIT_KEYS)
    amount, amount_source = extract_amount(hit)
    budget_source = extract_budget_source(hit)
    supplier_name, supplier_tax_id, supplier_name_source, supplier_tax_id_source = (
        extract_supplier_fields(hit)
    )
    organization_id = first_present(hit, ORG_ID_KEYS)
    title, title_source = first_subject(hit)
    issue_date = normalize_date(first_present(hit, ISSUE_DATE_KEYS))
    protocol_number = normalize_text(first_present(hit, PROTOCOL_NUMBER_KEYS))

    normalized = {
        "subject": title,
        "title": title,
        "title_source": title_source,
        "missing_subject": title is None,
        "issue_date": issue_date,
        "decision_date": issue_date,
        "protocol_number": protocol_number,
        "ada": ada,
        "decision_type_raw": decision_type_raw,
        "decision_type": decision_type,
        "decision_type_label": decision_type,
        "signer": signer,
        "unit": unit,
        "signer_ids": extract_ids(hit, SIGNER_ID_KEYS),
        "unit_ids": extract_ids(hit, UNIT_ID_KEYS),
        "organization_id": organization_id,
        "amount": amount,
        "amount_source": f"export:{amount_source}" if amount_source else None,
        "budget_source": budget_source,
        "supplier_name": supplier_name,
        "supplier_name_source": (
            f"export:{supplier_name_source}" if supplier_name_source else None
        ),
        "supplier_tax_id": supplier_tax_id,
        "supplier_tax_id_source": (
            f"export:{supplier_tax_id_source}" if supplier_tax_id_source else None
        ),
        "raw_extra_fields": hit.get("extraFieldValues"),
        "raw_detail_extra_fields": None,
        "enriched_from_full_record": False,
        "url": decision_url(ada, hit),
        "category": categorize(hit, decision_type_raw, decision_type_label),
        "canonical_id": stable_id(
            LAMIA_ORG_UID, issue_date, ada, prefix="lamia-decision"
        ),
        "duplicate_flag": False,
        "duplicate_of": None,
        "duplicate_group_size": 1,
    }
    return normalized


def missing_required_fields(item: dict[str, Any]) -> list[str]:
    return [field for field in REQUIRED_DECISION_FIELDS if not item.get(field)]


def decision_richness_score(item: dict[str, Any]) -> tuple[int, int, int, int]:
    """Rank duplicate ADA rows so the most complete row becomes canonical."""
    priority_fields = (
        "subject",
        "title",
        "issue_date",
        "decision_date",
        "decision_type",
        "decision_type_raw",
        "protocol_number",
        "amount",
        "budget_source",
        "supplier_name",
        "supplier_tax_id",
        "signer",
        "unit",
        "organization_id",
        "url",
    )
    priority_count = sum(
        1 for field in priority_fields if item.get(field) not in (None, "", [])
    )
    populated_count = sum(1 for value in item.values() if value not in (None, "", []))
    list_items = sum(len(value) for value in item.values() if isinstance(value, list))
    enriched = 1 if item.get("enriched_from_full_record") else 0
    return priority_count, populated_count, list_items, enriched


def merge_decision_records(primary: dict[str, Any], duplicate: dict[str, Any]) -> None:
    """Keep one row per ADA while preserving fields found on duplicate hits."""
    for key, value in duplicate.items():
        if value in (None, "", []):
            continue
        if primary.get(key) in (None, "", []):
            primary[key] = value

    if primary.get("subject") and not primary.get("title"):
        primary["title"] = primary["subject"]
    if primary.get("title") and not primary.get("subject"):
        primary["subject"] = primary["title"]
    if primary.get("issue_date") and not primary.get("decision_date"):
        primary["decision_date"] = primary["issue_date"]
    if primary.get("decision_date") and not primary.get("issue_date"):
        primary["issue_date"] = primary["decision_date"]


def deduplicate_decisions_by_ada(
    decisions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Return decisions with exact ADA duplicates collapsed to the richest row."""
    unique: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    duplicate_rows = 0
    malformed_without_ada = 0

    for item in decisions:
        ada = normalize_text(item.get("ada"))
        if not ada:
            malformed_without_ada += 1
            unique.append(item)
            continue
        item["ada"] = ada
        existing = seen.get(ada)
        if existing is None:
            seen[ada] = item
            unique.append(item)
            continue

        duplicate_rows += 1
        if decision_richness_score(item) > decision_richness_score(existing):
            merge_decision_records(item, existing)
            seen[ada] = item
            unique[unique.index(existing)] = item
        else:
            merge_decision_records(existing, item)

    return unique, {
        "input_rows": len(decisions),
        "unique_rows": len(unique),
        "duplicate_ada_rows_removed": duplicate_rows,
        "malformed_without_ada": malformed_without_ada,
    }


def split_malformed_decisions(
    decisions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    for item in decisions:
        missing = missing_required_fields(item)
        if missing:
            malformed.append({"missing_fields": missing, "decision": item})
        else:
            valid.append(item)
    return valid, malformed


def assert_unique_adas(decisions: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in decisions:
        ada = str(item.get("ada") or "")
        if not ada:
            continue
        if ada in seen:
            duplicates.add(ada)
        seen.add(ada)
    if duplicates:
        raise AssertionError(
            f"Duplicate ADA entries in final decisions: {', '.join(sorted(duplicates))}"
        )


def likely_duplicate(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if not (has_amount(left.get("amount")) and has_amount(right.get("amount"))):
        return False
    if amount_bucket(left.get("amount")) != amount_bucket(right.get("amount")):
        return False
    if left.get("decision_date") != right.get("decision_date"):
        return False
    return (
        title_similarity(left.get("title"), right.get("title"))
        >= DUPLICATE_SIMILARITY_THRESHOLD
    )


def assign_procurement_groups(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    """Mark probable republications/duplicates and assign stable event ids."""
    parent = list(range(len(decisions)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    buckets: dict[tuple[str, str], list[int]] = {}
    for index, item in enumerate(decisions):
        if not is_procurement_decision(item):
            continue
        key = (str(item.get("decision_date") or ""), amount_bucket(item.get("amount")))
        if key[0] and key[1] != "noamount":
            buckets.setdefault(key, []).append(index)

    duplicate_pair_count = 0
    for indexes in buckets.values():
        for offset, left_index in enumerate(indexes):
            for right_index in indexes[offset + 1 :]:
                if likely_duplicate(decisions[left_index], decisions[right_index]):
                    union(left_index, right_index)
                    duplicate_pair_count += 1

    groups: dict[int, list[int]] = {}
    for index in range(len(decisions)):
        groups.setdefault(find(index), []).append(index)

    duplicate_groups = 0
    duplicate_rows = 0
    for indexes in groups.values():
        group_items = [decisions[index] for index in indexes]
        procurement_group = any(is_procurement_decision(item) for item in group_items)
        canonical_basis = sorted(
            group_items,
            key=lambda item: (
                str(item.get("decision_date") or ""),
                amount_bucket(item.get("amount")),
                compact_text(item.get("title")),
                str(item.get("ada") or ""),
            ),
        )[0]
        canonical_id = stable_id(
            LAMIA_ORG_UID,
            canonical_basis.get("decision_date"),
            amount_bucket(canonical_basis.get("amount")),
            compact_text(canonical_basis.get("title")),
            prefix="lamia-procurement" if procurement_group else "lamia-decision",
        )
        canonical_ada = canonical_basis.get("ada")

        for item in group_items:
            item["canonical_id"] = canonical_id
            item["duplicate_group_size"] = len(group_items)
            item["duplicate_flag"] = (
                len(group_items) > 1 and item is not canonical_basis
            )
            item["duplicate_of"] = canonical_ada if item["duplicate_flag"] else None
            item["procurement_flag"] = procurement_group

        if len(group_items) > 1:
            duplicate_groups += 1
            duplicate_rows += len(group_items) - 1

    return {
        "duplicate_pair_matches": duplicate_pair_count,
        "duplicate_groups": duplicate_groups,
        "duplicate_rows": duplicate_rows,
        "procurement_events": len(
            {
                item.get("canonical_id")
                for item in decisions
                if item.get("procurement_flag")
            }
        ),
    }


def supplier_group_key(item: dict[str, Any]) -> str | None:
    tax_id = normalize_tax_id(item.get("supplier_tax_id"))
    if tax_id:
        return f"tax:{tax_id}"
    name = normalize_text(item.get("supplier_name"))
    if name:
        return f"name:{canonical_text(name)}"
    return None


def build_top_suppliers(
    decisions: list[dict[str, Any]], limit: int = TOP_SUPPLIERS_LIMIT
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in decisions:
        key = supplier_group_key(item)
        if not key:
            continue

        group = grouped.setdefault(
            key,
            {
                "supplier_name": item.get("supplier_name"),
                "supplier_tax_id": item.get("supplier_tax_id"),
                "decision_count": 0,
                "amount_decision_count": 0,
                "total_amount": 0.0,
                "adas": [],
            },
        )
        if item.get("supplier_name") and not group.get("supplier_name"):
            group["supplier_name"] = item.get("supplier_name")
        if item.get("supplier_tax_id") and not group.get("supplier_tax_id"):
            group["supplier_tax_id"] = item.get("supplier_tax_id")
        group["decision_count"] += 1
        if item.get("ada"):
            group["adas"].append(item.get("ada"))
        if has_amount(item.get("amount")):
            group["total_amount"] += float(item.get("amount"))
            group["amount_decision_count"] += 1

    summaries = list(grouped.values())
    for group in summaries:
        group["total_amount"] = round(group["total_amount"], 2)

    by_amount = sorted(
        summaries,
        key=lambda item: (
            float(item.get("total_amount") or 0),
            int(item.get("amount_decision_count") or 0),
            int(item.get("decision_count") or 0),
            str(item.get("supplier_name") or ""),
        ),
        reverse=True,
    )[:limit]
    by_count = sorted(
        summaries,
        key=lambda item: (
            int(item.get("decision_count") or 0),
            float(item.get("total_amount") or 0),
            str(item.get("supplier_name") or ""),
        ),
        reverse=True,
    )[:limit]
    return by_amount, by_count


def build_top_procurements(
    decisions: list[dict[str, Any]], limit: int = TOP_PROCUREMENTS_LIMIT
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in decisions:
        if is_procurement_decision(item) and has_amount(item.get("amount")):
            grouped.setdefault(str(item.get("canonical_id")), []).append(item)

    summaries: list[dict[str, Any]] = []
    for canonical_id, items in grouped.items():
        primary = sorted(
            items,
            key=lambda item: (
                bool(item.get("duplicate_flag")),
                str(item.get("decision_date") or ""),
                str(item.get("ada") or ""),
            ),
        )[0]
        summaries.append(
            {
                "canonical_id": canonical_id,
                "decision_date": primary.get("decision_date"),
                "ada": primary.get("ada"),
                "title": primary.get("title"),
                "amount": primary.get("amount"),
                "decision_type": primary.get("decision_type"),
                "budget_source": primary.get("budget_source"),
                "supplier_name": primary.get("supplier_name"),
                "supplier_tax_id": primary.get("supplier_tax_id"),
                "url": primary.get("url"),
                "duplicate_count": max(0, len(items) - 1),
            }
        )

    return sorted(
        summaries,
        key=lambda item: (
            float(item.get("amount") or 0),
            str(item.get("decision_date") or ""),
        ),
        reverse=True,
    )[:limit]


def write_json(path: Path, payload: dict[str, Any]) -> None:
    decisions = payload.get("decisions")
    if isinstance(decisions, list):
        assert_unique_adas([item for item in decisions if isinstance(item, dict)])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


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

    enrichment_summary = metadata.get("enrichment_summary") or {}
    duplicate_summary = metadata.get("duplicate_summary") or {}
    if enrichment_summary or duplicate_summary:
        lines.extend(
            [
                "## Semantic quality notes",
                "",
                f"- Empty subjects before detail enrichment: {enrichment_summary.get('subjects_missing_before_enrichment', '—')}",
                f"- Empty subjects after detail enrichment: {enrichment_summary.get('subjects_missing_after_enrichment', '—')}",
                f"- Exact duplicate ADA rows removed: {duplicate_summary.get('duplicate_ada_rows_removed', '—')}",
                f"- Likely duplicate/republication groups: {duplicate_summary.get('duplicate_groups', '—')}",
                f"- Duplicate rows flagged: {duplicate_summary.get('duplicate_rows', '—')}",
                "",
            ]
        )

    top_procurements = payload.get("top_procurements") or []
    if top_procurements:
        lines.extend(
            [
                "## Top procurements / contracts",
                "",
                "| Rank | Canonical event | Date | ΑΔΑ | Type | Title | Supplier | Amount | Duplicates | URL |",
                "| ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | --- |",
            ]
        )
        for rank, item in enumerate(top_procurements, start=1):
            url = item.get("url")
            ada = item.get("ada")
            ada_text = f"[{ada}]({url})" if ada and url else markdown_cell(ada)
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(rank),
                        markdown_cell(item.get("canonical_id")),
                        markdown_cell(item.get("decision_date")),
                        ada_text,
                        markdown_cell(item.get("decision_type")),
                        markdown_cell(item.get("title")),
                        markdown_cell(
                            item.get("supplier_name") or item.get("supplier_tax_id")
                        ),
                        format_amount(item.get("amount")),
                        str(item.get("duplicate_count", 0)),
                        markdown_cell(url),
                    ]
                )
                + " |"
            )
        lines.append("")

    top_suppliers_by_amount = payload.get("top_suppliers_by_amount") or []
    if top_suppliers_by_amount:
        lines.extend(
            [
                "## Top Suppliers by Amount",
                "",
                "| Rank | Supplier | Tax ID | Total amount | Amount-bearing decisions | Decisions | ΑΔΑ samples |",
                "| ---: | --- | --- | ---: | ---: | ---: | --- |",
            ]
        )
        for rank, item in enumerate(top_suppliers_by_amount, start=1):
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(rank),
                        markdown_cell(item.get("supplier_name")),
                        markdown_cell(item.get("supplier_tax_id")),
                        format_amount(item.get("total_amount")),
                        str(item.get("amount_decision_count", 0)),
                        str(item.get("decision_count", 0)),
                        markdown_cell(
                            ", ".join(str(ada) for ada in item.get("adas", [])[:5])
                        ),
                    ]
                )
                + " |"
            )
        lines.append("")

    top_suppliers_by_count = payload.get("top_suppliers_by_count") or []
    if top_suppliers_by_count:
        lines.extend(
            [
                "## Top Suppliers by Count",
                "",
                "| Rank | Supplier | Tax ID | Decisions | Total amount | Amount-bearing decisions | ΑΔΑ samples |",
                "| ---: | --- | --- | ---: | ---: | ---: | --- |",
            ]
        )
        for rank, item in enumerate(top_suppliers_by_count, start=1):
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(rank),
                        markdown_cell(item.get("supplier_name")),
                        markdown_cell(item.get("supplier_tax_id")),
                        str(item.get("decision_count", 0)),
                        format_amount(item.get("total_amount")),
                        str(item.get("amount_decision_count", 0)),
                        markdown_cell(
                            ", ".join(str(ada) for ada in item.get("adas", [])[:5])
                        ),
                    ]
                )
                + " |"
            )
        lines.append("")

    if not decisions:
        lines.extend(["No decisions found for this period.", ""])
    else:
        lines.extend(
            [
                "## Decisions",
                "",
                "| Date | ΑΔΑ | Canonical event | Duplicate? | Category | Type | Raw type | Title | Supplier | Tax ID | Signer / Unit | Amount | Budget source | URL |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | --- |",
            ]
        )
        for item in decisions:
            signer_unit = (
                " / ".join(
                    part for part in (item.get("signer"), item.get("unit")) if part
                )
                or None
            )
            url = item.get("url")
            ada = item.get("ada")
            ada_text = f"[{ada}]({url})" if ada and url else markdown_cell(ada)
            lines.append(
                "| "
                + " | ".join(
                    [
                        markdown_cell(item.get("decision_date")),
                        ada_text,
                        markdown_cell(item.get("canonical_id")),
                        "yes" if item.get("duplicate_flag") else "no",
                        markdown_cell(item.get("category")),
                        markdown_cell(item.get("decision_type")),
                        markdown_cell(item.get("decision_type_raw")),
                        markdown_cell(item.get("title")),
                        markdown_cell(item.get("supplier_name")),
                        markdown_cell(item.get("supplier_tax_id")),
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
    parser = argparse.ArgumentParser(
        description="Build a Lamia Municipality Diavgeia digest"
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        type=parse_iso_date,
        default=default_from,
        help="Start date (YYYY-MM-DD). Defaults to the first day of the previous month.",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        type=parse_iso_date,
        default=default_to,
        help="End date (YYYY-MM-DD). Defaults to the last day of the previous month.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("LAMIA_DIGEST_LIMIT", "500")),
        help="Maximum decisions to fetch.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Diavgeia page size for export requests.",
    )
    parser.add_argument(
        "--timeout", type=int, default=60, help="HTTP timeout in seconds."
    )
    parser.add_argument(
        "--detail-timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds for full-record enrichment requests.",
    )
    parser.add_argument(
        "--max-detail-fetches",
        type=int,
        default=None,
        help="Optional cap on full-record enrichment attempts for decisions missing amounts.",
    )
    enrich_group = parser.add_mutually_exclusive_group()
    enrich_group.add_argument(
        "--enrich-details",
        dest="enrich_details",
        action="store_true",
        default=True,
        help="Fetch full decision records when export records do not include an amount (default).",
    )
    enrich_group.add_argument(
        "--no-enrich-details",
        dest="enrich_details",
        action="store_false",
        help="Disable full-record enrichment for missing amounts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for Lamia artifacts.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print query, enrichment summary, and output paths.",
    )
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
        raw_decisions = fetch_export(
            query, limit=args.limit, page_size=args.page_size, timeout=args.timeout
        )
    except requests.RequestException as exc:
        print(f"Failed to fetch Lamia decisions from Diavgeia: {exc}", file=sys.stderr)
        return 1

    normalized_decisions = [normalize_decision(item) for item in raw_decisions]
    enrichment_summary = enrich_missing_amounts(
        normalized_decisions,
        enabled=args.enrich_details,
        timeout=args.detail_timeout,
        max_fetches=args.max_detail_fetches,
    )
    decisions, ada_dedup_summary = deduplicate_decisions_by_ada(normalized_decisions)
    decisions, malformed_decisions = split_malformed_decisions(decisions)
    assert_unique_adas(decisions)
    duplicate_summary = assign_procurement_groups(decisions)
    duplicate_summary.update(ada_dedup_summary)
    top_procurements = build_top_procurements(decisions)
    top_suppliers_by_amount, top_suppliers_by_count = build_top_suppliers(decisions)

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
            "duplicate_summary": duplicate_summary,
            "malformed_decision_count": len(malformed_decisions),
        },
        "top_procurements": top_procurements,
        "top_suppliers_by_amount": top_suppliers_by_amount,
        "top_suppliers_by_count": top_suppliers_by_count,
        "decisions": decisions,
    }

    json_path = args.output_dir / "lamia_digest.json"
    md_path = args.output_dir / "lamia_digest.md"
    malformed_path = args.output_dir / "lamia_digest_malformed.json"
    write_json(json_path, payload)
    write_json(
        malformed_path,
        {"metadata": payload["metadata"], "malformed_decisions": malformed_decisions},
    )
    write_markdown(md_path, payload)

    if malformed_decisions:
        print(
            f"Warning: wrote {len(malformed_decisions)} malformed Lamia decisions to {malformed_path}",
            file=sys.stderr,
        )
    print(f"Wrote {len(decisions)} Lamia decisions to {json_path} and {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
