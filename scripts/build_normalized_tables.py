#!/usr/bin/env python3
"""Build normalized analytics tables from cached Diavgeia JSON files.

The script is intentionally offline-only: it reads monthly cache folders created
by the digest/backfill workflows and never calls the Diavgeia API.
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import importlib.util
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd

DEFAULT_RAW_ROOT = Path("data/raw/diavgeia")
DEFAULT_OUTPUT_ROOT = Path("data/normalized")
DETAIL_URL_TEMPLATE = "https://diavgeia.gov.gr/opendata/decisions/{ada}"
OUTPUT_FORMATS = ("parquet", "csv")
PARQUET_ENGINE_MISSING_MESSAGE = "Parquet engine missing. Re-run with --format csv"

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
    "Δ.2.1": "Open procurement notice",
    "Δ.2.2": "Contract award",
    "Δ.2.3": "Contract signing",
    "Δ.2.4": "Procurement cancellation",
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
URL_KEYS = ("documentUrl", "url", "decisionUrl", "document_url")
SIGNER_KEYS = (
    "signerName",
    "signer",
    "signedBy",
    "signed_by",
    "finalSigner",
    "finalSignerName",
    "issuerName",
)
UNIT_KEYS = (
    "unitName",
    "unitLabel",
    "unit",
    "unitDescription",
    "organizationalUnit",
    "organizationalUnitLabel",
    "organizationUnit",
    "organizationUnitLabel",
)
AMOUNT_KEYS = (
    "amount",
    "paymentAmount",
    "expenseAmount",
    "netAmount",
    "totalAmount",
    "amountWithVAT",
    "budgetAmount",
)
# These fields frequently contain identifiers, dates, budget codes, and other
# narrative text. Even when their value is a nested object with amount-like
# child keys, they are not trusted amount containers.
UNTRUSTED_AMOUNT_TEXT_KEYS = (*SUBJECT_KEYS, *URL_KEYS, *SIGNER_KEYS, *UNIT_KEYS)
AMOUNT_WARNING_THRESHOLD = 10_000_000
LOW_AMOUNT_COVERAGE_THRESHOLD = 0.5
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
    "afm",
    "AFM",
    "vatNumber",
    "vatId",
    "taxId",
    "taxNumber",
    "tin",
)
EXTRA_FIELD_LABEL_KEYS = ("label", "name", "field", "fieldName", "fieldLabel", "field_label", "key", "title")
EXTRA_FIELD_VALUE_KEYS = ("value", "fieldValue", "field_value", "values", "amount")
AMOUNT_LABEL_TOKENS = ("ποσο", "amount", "paymentamount", "expenseamount")
SUPPLIER_NAME_LABEL_TOKENS = (
    "supplier",
    "vendor",
    "contractor",
    "counterparty",
    "beneficiary",
    "recipient",
    "payee",
    "αναδοχος",
    "αναδοχου",
    "προμηθευτης",
    "προμηθευτη",
    "δικαιουχος",
    "δικαιουχου",
    "επωνυμια",
)
SUPPLIER_TAX_ID_LABEL_TOKENS = ("afm", "αφμ", "taxid", "taxnumber", "vatnumber", "vatid", "tin")
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

DECISION_COLUMNS = [
    "org",
    "year",
    "month",
    "procurement_stage",
    "ada",
    "issue_date",
    "decision_type",
    "subject",
    "url",
    "amount",
    "amount_source",
    "supplier_name",
    "supplier_tax_id",
    "signer",
    "unit",
]
SUPPLIER_COLUMNS = [
    "supplier_key",
    "supplier_name_normalized",
    "supplier_tax_id",
    "first_seen",
    "last_seen",
    "decision_count",
    "total_amount",
]
PROCUREMENT_COLUMNS = [
    "procurement_key",
    "org",
    "year",
    "month",
    "ada",
    "issue_date",
    "decision_type",
    "subject",
    "amount",
    "amount_source",
    "supplier_key",
    "supplier_name",
    "supplier_tax_id",
    "signer",
    "unit",
    "url",
]
MONTHLY_SUMMARY_COLUMNS = [
    "year",
    "month",
    "decision_count",
    "amount_total",
    "supplier_count",
    "amount_known_count",
    "amount_missing_count",
    "supplier_known_count",
    "supplier_missing_count",
    "detail_enriched_decision_count",
    "search_only_decision_count",
]
INTERNAL_DECISION_COLUMNS = [*DECISION_COLUMNS, "_detail_enriched"]


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def extract_export_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("decisionResultList", "decisions", "diavgeia_decisions"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
    decision_results = payload.get("decisionResults") or payload.get("decisionresults")
    if isinstance(decision_results, dict):
        rows = decision_results.get("decision") or decision_results.get("decisions") or []
        if isinstance(rows, dict):
            return [rows]
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
    return []


def unwrap_detail(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    for key in ("decision", "decisionResult", "data"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def normalize_text(value: Any) -> str | None:
    if value in (None, "", []):
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


@functools.lru_cache(maxsize=8192)
def normalize_label(value: Any) -> str:
    text = str(value).strip().lower()
    decomposed = unicodedata.normalize("NFD", text)
    without_accents = "".join(char for char in decomposed if unicodedata.category(char) != "Mn")
    return re.sub(r"[\s_\-]+", "", without_accents)


@functools.lru_cache(maxsize=8192)
def canonical_text(value: Any) -> str:
    if value in (None, "", []):
        return ""
    if isinstance(value, float) and value != value:
        return ""
    text = unicodedata.normalize("NFD", str(value).lower())
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^0-9a-zα-ω]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


CANONICAL_PROCUREMENT_TOKENS = tuple(canonical_text(token) for token in PROCUREMENT_TOKENS)


def normalize_date(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:19], fmt).date().isoformat()
        except ValueError:
            continue
    return text


def normalize_amount(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool) or isinstance(value, (dict, list)):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value == value else None
    normalized = str(value).strip().replace("€", "").replace(" ", "")
    if not normalized:
        return None
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


def first_present(source: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, "", []):
            return value
    return None


def text_from_named_value(value: Any) -> str | None:
    if isinstance(value, dict):
        first_name = normalize_text(value.get("firstName") or value.get("firstname"))
        last_name = normalize_text(value.get("lastName") or value.get("lastname") or value.get("surname"))
        if first_name or last_name:
            return " ".join(part for part in (first_name, last_name) if part)
        for key in ("name", "label", "title", "description", "fullName", "displayName", *EXTRA_FIELD_VALUE_KEYS):
            text = text_from_named_value(value.get(key))
            if text:
                return text
        return None
    if isinstance(value, list):
        texts = [text_from_named_value(item) for item in value]
        texts = [text for text in texts if text]
        return ", ".join(texts) if texts else None
    return normalize_text(value)


def is_amount_label(value: Any) -> bool:
    label = normalize_label(value)
    return any(token in label for token in AMOUNT_LABEL_TOKENS)


def is_untrusted_amount_text_key(value: Any) -> bool:
    label = normalize_label(value)
    return any(label == normalize_label(key) for key in UNTRUSTED_AMOUNT_TEXT_KEYS)


def is_supplier_name_label(value: Any) -> bool:
    label = normalize_label(value)
    return any(normalize_label(token) in label for token in SUPPLIER_NAME_LABEL_TOKENS) and not is_supplier_tax_id_label(value)


def is_supplier_tax_id_label(value: Any) -> bool:
    label = normalize_label(value)
    return "amountwithvat" not in label and any(token in label for token in SUPPLIER_TAX_ID_LABEL_TOKENS)


def extract_text_by_keys_or_labels(source: Any, keys: tuple[str, ...], label_tokens: tuple[str, ...]) -> str | None:
    if isinstance(source, dict):
        for key in keys:
            if key in source:
                text = text_from_named_value(source.get(key))
                if text:
                    return text
        labels = [source.get(key) for key in EXTRA_FIELD_LABEL_KEYS if source.get(key) not in (None, "", [])]
        if any(any(normalize_label(token) in normalize_label(label) for token in label_tokens) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                text = text_from_named_value(source.get(key))
                if text:
                    return text
        extra_fields = source.get("extraFieldValues")
        if extra_fields not in (None, "", []):
            text = extract_text_by_keys_or_labels(extra_fields, keys, label_tokens)
            if text:
                return text
        for key, value in source.items():
            if key == "extraFieldValues":
                continue
            key_label = normalize_label(key)
            if any(normalize_label(token) in key_label for token in label_tokens):
                text = text_from_named_value(value)
                if text:
                    return text
        for value in source.values():
            if isinstance(value, (dict, list)):
                text = extract_text_by_keys_or_labels(value, keys, label_tokens)
                if text:
                    return text
    elif isinstance(source, list):
        for item in source:
            text = extract_text_by_keys_or_labels(item, keys, label_tokens)
            if text:
                return text
    return None


def _extract_structured_amount(source: Any, path: str = "") -> tuple[float | None, str | None]:
    """Extract amounts only from trusted structured amount fields.

    The Diavgeia search and detail payloads contain subjects and other free-text
    strings with long identifiers, dates, and budget references.  Those strings
    are not safe amount sources, so this helper only inspects explicit amount
    keys or labeled extra-field values.
    """
    if isinstance(source, dict):
        labels = [source.get(key) for key in EXTRA_FIELD_LABEL_KEYS if source.get(key) not in (None, "", [])]
        if any(is_amount_label(label) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                amount = normalize_amount(source.get(key))
                if amount is not None:
                    label_text = normalize_text(labels[0]) or "amount"
                    source_name = f"{path}.extraFieldValues[{label_text}]" if path else f"extraFieldValues[{label_text}]"
                    return amount, source_name
        for key, value in source.items():
            child_path = f"{path}.{key}" if path else str(key)
            if is_untrusted_amount_text_key(key):
                continue
            if key in AMOUNT_KEYS or is_amount_label(key):
                amount = normalize_amount(value)
                if amount is not None:
                    return amount, child_path
                # Value may be a nested dict like awardAmount: {amount: X, currency: Y}
                if isinstance(value, (dict, list)):
                    amount, amount_source = _extract_structured_amount(value, child_path)
                    if amount is not None:
                        return amount, amount_source
            elif isinstance(value, (dict, list)):
                amount, amount_source = _extract_structured_amount(value, child_path)
                if amount is not None:
                    return amount, amount_source
    elif isinstance(source, list):
        for index, item in enumerate(source):
            amount, amount_source = _extract_structured_amount(item, f"{path}[{index}]" if path else f"[{index}]")
            if amount is not None:
                return amount, amount_source
    return None, None


def extract_amount(source: Any) -> float | None:
    amount, _amount_source = _extract_structured_amount(source)
    return amount


def extract_amount_with_source(source: Any, source_prefix: str) -> tuple[float | None, str | None]:
    amount, amount_source = _extract_structured_amount(source)
    if amount is None:
        return None, None
    return amount, f"{source_prefix}:{amount_source}" if amount_source else source_prefix


def normalize_tax_id(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    compact = re.sub(r"[^0-9A-Za-z]", "", text).upper()
    if not compact or compact == "EL":
        return None
    full_match = re.fullmatch(r"(?:EL)?(\d{9})", compact)
    if full_match:
        return full_match.group(1)
    el_match = re.search(r"EL(\d{9})(?!\d)", compact)
    if el_match:
        return el_match.group(1)
    digit_sequences = re.findall(r"\d+", text)
    if len(digit_sequences) == 1 and len(digit_sequences[0]) == 9:
        return digit_sequences[0]
    return None


def extract_supplier_fields(source: Any) -> tuple[str | None, str | None]:
    name = None
    tax_id = None
    if isinstance(source, dict):
        labels = [source.get(key) for key in EXTRA_FIELD_LABEL_KEYS if source.get(key) not in (None, "", [])]
        if any(is_supplier_name_label(label) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                name = text_from_named_value(source.get(key))
                if name:
                    break
        if any(is_supplier_tax_id_label(label) for label in labels):
            for key in EXTRA_FIELD_VALUE_KEYS:
                tax_id = normalize_tax_id(source.get(key))
                if tax_id:
                    break
        for key, value in source.items():
            if not name and (key in SUPPLIER_NAME_KEYS or is_supplier_name_label(key)):
                name = text_from_named_value(value)
            if not tax_id and (key in SUPPLIER_TAX_ID_KEYS or is_supplier_tax_id_label(key)):
                tax_id = normalize_tax_id(value)
            if name and tax_id:
                return name, tax_id
        for value in source.values():
            if isinstance(value, (dict, list)):
                nested_name, nested_tax_id = extract_supplier_fields(value)
                name = name or nested_name
                tax_id = tax_id or nested_tax_id
                if name and tax_id:
                    return name, tax_id
    elif isinstance(source, list):
        for item in source:
            nested_name, nested_tax_id = extract_supplier_fields(item)
            name = name or nested_name
            tax_id = tax_id or nested_tax_id
            if name and tax_id:
                return name, tax_id
    return name, tax_id


def normalized_decision_type(raw_type: Any, label: Any = None) -> str | None:
    label_text = normalize_text(label)
    if label_text:
        return label_text
    raw_text = normalize_text(raw_type)
    if not raw_text:
        return None
    return DECISION_TYPE_LABELS.get(raw_text, raw_text)


def supplier_name_normalized(value: Any) -> str | None:
    text = canonical_text(value)
    return text.upper() if text else None


def supplier_key(name: Any, tax_id: Any) -> str | None:
    if tax_id in (None, "", []):
        normalized_tax_id = None
    else:
        normalized_tax_id = normalize_tax_id(tax_id)
    if normalized_tax_id:
        return f"tax:{normalized_tax_id}"
    if name in (None, "", []):
        return None
    normalized_name = supplier_name_normalized(name)
    if normalized_name:
        digest = hashlib.sha1(normalized_name.encode("utf-8")).hexdigest()[:12]
        return f"name:{digest}"
    return None


def detail_url(ada: str | None) -> str | None:
    if not ada:
        return None
    return DETAIL_URL_TEMPLATE.format(ada=quote(str(ada), safe=""))


def coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None



def extract_subject_budget_amount(subject: Any) -> tuple[float | None, str | None]:
    text = normalize_text(subject) or ""
    canon = canonical_text(text)

    trusted_markers = (
        "προυπ",
        "προυπολογισμου",
        "προυπολ",
        "προυπολογισμος",
        "μελετης",
        "με φπα",
    )
    if not any(marker in canon for marker in trusted_markers):
        return None, None

    matches = re.findall(r"\d{1,3}(?:[.\s]\d{3})+(?:,\d{2})?|\d+(?:,\d{2})", text)
    amounts = []
    for raw in matches:
        amount = normalize_amount(raw)
        if amount is not None and 1 <= amount <= 10_000_000:
            amounts.append(amount)

    if not amounts:
        return None, None

    return max(amounts), "subject:trusted_budget_phrase"


def normalize_decision(org: str, year: int, month: int, export_row: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any]:
    detail_enriched = bool(detail)
    detail = detail or {}
    combined = {**export_row, **detail}
    ada = normalize_text(coalesce(detail.get("ada"), export_row.get("ada"), export_row.get("ADA")))
    subject = text_from_named_value(coalesce(first_present(detail, SUBJECT_KEYS), first_present(export_row, SUBJECT_KEYS)))
    issue_date = normalize_date(coalesce(first_present(detail, ISSUE_DATE_KEYS), first_present(export_row, ISSUE_DATE_KEYS)))
    raw_type = coalesce(first_present(detail, DECISION_TYPE_KEYS), first_present(export_row, DECISION_TYPE_KEYS))
    type_label = coalesce(first_present(detail, DECISION_TYPE_LABEL_KEYS), first_present(export_row, DECISION_TYPE_LABEL_KEYS))
    amount, amount_source = extract_amount_with_source(detail, "detail") if detail else (None, None)
    if amount is None:
        amount, amount_source = extract_amount_with_source(export_row, "search_export")
    if amount is None:
        amount, amount_source = extract_subject_budget_amount(subject)
    detail_supplier_name, detail_supplier_tax_id = extract_supplier_fields(detail)
    export_supplier_name, export_supplier_tax_id = extract_supplier_fields(export_row)
    supplier_name = normalize_text(detail_supplier_name or export_supplier_name)
    supplier_tax_id = normalize_tax_id(detail_supplier_tax_id or export_supplier_tax_id)
    url = normalize_text(coalesce(first_present(detail, URL_KEYS), first_present(export_row, URL_KEYS), detail_url(ada)))
    signer = extract_text_by_keys_or_labels(combined, SIGNER_KEYS, ("signer", "signedby", "finalsigner", "υπογραφων", "υπογραφη"))
    unit = extract_text_by_keys_or_labels(combined, UNIT_KEYS, ("unit", "organizationunit", "organizationalunit", "μοναδα", "τμημα", "διευθυνση"))
    procurement_stage = classify_procurement_stage(subject)
    return {
        "org": str(org),
        "year": int(year),
        "month": int(month),
        "ada": ada,
        "issue_date": issue_date,
        "decision_type": normalized_decision_type(raw_type, type_label),
        "procurement_stage": procurement_stage,
	"subject": subject,
        "url": url,
        "amount": amount,
        "amount_source": amount_source,
        "supplier_name": supplier_name,
        "supplier_tax_id": supplier_tax_id,
        "signer": signer,
        "unit": unit,
        "_detail_enriched": detail_enriched,
    }


def parse_partition_value(path: Path, prefix: str) -> str | None:
    for part in path.parts:
        if part.startswith(prefix):
            return part.split("=", 1)[1]
    return None


def load_decisions(raw_root: Path, org: str, limit_months: int | None = None) -> list[dict[str, Any]]:
    org_dir = raw_root / f"organization_uid={org}"
    if not org_dir.exists():
        return []

    decisions: list[dict[str, Any]] = []
    search_paths = sorted(org_dir.glob("year=*/month=*/search_export.json"))
    if limit_months is not None:
        search_paths = search_paths[:limit_months]
    for search_path in search_paths:
        year_text = parse_partition_value(search_path, "year=")
        month_text = parse_partition_value(search_path, "month=")
        if not year_text or not month_text:
            continue
        year = int(year_text)
        month = int(month_text)
        month_dir = search_path.parent
        export_rows = extract_export_rows(read_json(search_path))
        detail_by_ada: dict[str, dict[str, Any]] = {}
        decisions_dir = month_dir / "decisions"
        if decisions_dir.exists():
            for detail_path in sorted(decisions_dir.glob("*.json")):
                detail = unwrap_detail(read_json(detail_path))
                ada = normalize_text(detail.get("ada")) or detail_path.stem
                detail_by_ada[ada] = detail

        for row in export_rows:
            ada = normalize_text(row.get("ada") or row.get("ADA"))
            detail = detail_by_ada.get(ada) if ada else None
            decisions.append(normalize_decision(org, year, month, row, detail))
    return decisions


def procurement_searchable_text(decision: dict[str, Any]) -> str:
    decision_type = normalize_text(decision.get("decision_type")) or ""
    subject = normalize_text(decision.get("subject")) or ""
    return canonical_text(f"{decision_type} {subject}")

PROCUREMENT_STAGE_RULES = {
    "award": [
        "ανακηρυξη προσωρινου αναδοχου",
        "αναδοχος",
        "κατακυρωση",
    ],
    "committee": [
        "συγκροτηση επιτροπης",
        "ορισμος μελων",
    ],
    "approval": [
        "εγκριση",
        "καταρτιση ορων",
    ],
    "cancellation": [
        "ακυρωση",
        "ανακληση",
    ],
}


def classify_procurement_stage(text: str | None) -> str:
    text = canonical_text(text or "")

    for stage, patterns in PROCUREMENT_STAGE_RULES.items():
        if any(p in text for p in patterns):
            return stage

    return "other"
PAYROLL_ADMIN_TOKENS = (
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
)


def is_procurement(decision: dict[str, Any]) -> bool:
    text = decision.get("_procurement_searchable_text")
    if text is None:
        text = procurement_searchable_text(decision)

    non_procurement_tokens = (
        "μειωση εγγυησεων",
        "επιστροφη εγγυητικης",
        "συγκροτηση συνεργειου",
        "συσταση επιτροπης",
        "απορριψη",
        "μη εγκριση",
        "ακυρωση",
        "ανακληση",
        "ανακλησης",
        "ανακληση διακηρυξης",
        "κηρυξη προμηθευτη εκπτωτου",
        "ακυρωση παραστ",
        *PAYROLL_ADMIN_TOKENS,
    )
    padded = f" {text} "
    if any(f" {token}" in padded for token in non_procurement_tokens):
        return False

    supplier_present = bool(supplier_key(decision.get("supplier_name"), decision.get("supplier_tax_id")))
    amount = decision.get("amount")
    amount_present = amount is not None and pd.notna(amount)

    if amount_present and float(amount) > 10_000_000:
        return False

    return supplier_present or amount_present or any(token in text for token in CANONICAL_PROCUREMENT_TOKENS)


def build_tables(decisions: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    decisions_df = pd.DataFrame(decisions, columns=INTERNAL_DECISION_COLUMNS)
    if decisions_df.empty:
        return {
            "decisions": decisions_df[DECISION_COLUMNS],
            "suppliers": pd.DataFrame(columns=SUPPLIER_COLUMNS),
            "procurements": pd.DataFrame(columns=PROCUREMENT_COLUMNS),
            "monthly_summary": pd.DataFrame(columns=MONTHLY_SUMMARY_COLUMNS),
        }

    decisions_df["amount"] = pd.to_numeric(decisions_df["amount"], errors="coerce")
    decisions_df = decisions_df.drop_duplicates(subset=["ada"], keep="first")
    decisions_df = decisions_df.sort_values(["year", "month", "issue_date", "ada"], na_position="last").reset_index(drop=True)

    sorted_records = decisions_df.to_dict("records")
    decisions_df["_procurement_searchable_text"] = [procurement_searchable_text(row) for row in sorted_records]
    decisions_df["_supplier_key"] = [
        supplier_key(row.get("supplier_name"), row.get("supplier_tax_id")) for row in sorted_records
    ]
    decisions_df["_supplier_name_normalized"] = [
        supplier_name_normalized(name) for name in decisions_df["supplier_name"]
    ]
    decisions_df["_supplier_tax_id_normalized"] = [
        normalize_tax_id(tax_id) for tax_id in decisions_df["supplier_tax_id"]
    ]
    decision_records = decisions_df.to_dict("records")

    supplier_groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"decision_adas": set(), "total_amount": 0.0})
    for row in decision_records:
        key = row.get("_supplier_key")
        if key is None or pd.isna(key):
            continue
        group = supplier_groups[key]
        group["supplier_key"] = key
        group["supplier_name_normalized"] = group.get("supplier_name_normalized") or row.get("_supplier_name_normalized")
        group["supplier_tax_id"] = group.get("supplier_tax_id") or row.get("_supplier_tax_id_normalized")
        issue_date = row.get("issue_date")
        if issue_date:
            dates = [date for date in (group.get("first_seen"), group.get("last_seen"), issue_date) if date]
            group["first_seen"] = min(dates)
            group["last_seen"] = max(dates)
        if row.get("ada"):
            group["decision_adas"].add(row.get("ada"))
        if pd.notna(row.get("amount")):
            group["total_amount"] += float(row.get("amount"))

    supplier_rows = []
    for group in supplier_groups.values():
        supplier_rows.append(
            {
                "supplier_key": group.get("supplier_key"),
                "supplier_name_normalized": group.get("supplier_name_normalized"),
                "supplier_tax_id": group.get("supplier_tax_id"),
                "first_seen": group.get("first_seen"),
                "last_seen": group.get("last_seen"),
                "decision_count": len(group.get("decision_adas", set())),
                "total_amount": group.get("total_amount", 0.0),
            }
        )
    suppliers_df = pd.DataFrame(supplier_rows, columns=SUPPLIER_COLUMNS).sort_values("supplier_key").reset_index(drop=True)

    procurement_rows = []
    for row in decision_records:
        if not is_procurement(row):
            continue
        key = row.get("_supplier_key")
        if key is not None and pd.isna(key):
            key = None
        procurement_id_source = row.get("ada") or "|".join(str(row.get(col) or "") for col in ("org", "year", "month", "subject", "amount"))
        procurement_rows.append(
            {
                "procurement_key": f"proc:{hashlib.sha1(str(procurement_id_source).encode('utf-8')).hexdigest()[:12]}",
                "org": row.get("org"),
                "year": row.get("year"),
                "month": row.get("month"),
                "ada": row.get("ada"),
                "issue_date": row.get("issue_date"),
                "decision_type": row.get("decision_type"),
                "subject": row.get("subject"),
                "amount": row.get("amount"),
                "amount_source": row.get("amount_source"),
                "supplier_key": key,
                "supplier_name": row.get("supplier_name"),
                "supplier_tax_id": row.get("supplier_tax_id"),
                "signer": row.get("signer"),
                "unit": row.get("unit"),
                "url": row.get("url"),
            }
        )
    procurements_df = pd.DataFrame(procurement_rows, columns=PROCUREMENT_COLUMNS)

    procurement_month_groups = {
        key: group for key, group in procurements_df.groupby(["year", "month"], dropna=False)
    }

    summary_rows = []
    for (year, month), group in decisions_df.groupby(["year", "month"], dropna=False):
        pgroup = procurement_month_groups.get((year, month), pd.DataFrame(columns=PROCUREMENT_COLUMNS))

        supplier_count = int(pgroup["supplier_key"].dropna().nunique()) if not pgroup.empty else 0
        amount_known_count = int(pgroup["amount"].notna().sum()) if not pgroup.empty else 0
        row_count = int(len(group))
        supplier_known_count = int(pgroup["supplier_key"].notna().sum()) if not pgroup.empty else 0
        detail_enriched_count = int(group["_detail_enriched"].fillna(False).astype(bool).sum())
        summary_rows.append(
            {
                "year": int(year),
                "month": int(month),
                "decision_count": int(group["ada"].nunique(dropna=True) or row_count),
                "amount_total": float(pgroup["amount"].fillna(0).sum()) if not pgroup.empty else 0.0,
                "supplier_count": supplier_count,
                "amount_known_count": amount_known_count,
                "amount_missing_count": row_count - amount_known_count,
                "supplier_known_count": supplier_known_count,
                "supplier_missing_count": row_count - supplier_known_count,
                "detail_enriched_decision_count": detail_enriched_count,
                "search_only_decision_count": row_count - detail_enriched_count,
            }
        )
    monthly_summary_df = pd.DataFrame(summary_rows, columns=MONTHLY_SUMMARY_COLUMNS).sort_values(["year", "month"]).reset_index(drop=True)

    return {
        "decisions": decisions_df[DECISION_COLUMNS],
        "suppliers": suppliers_df,
        "procurements": procurements_df,
        "monthly_summary": monthly_summary_df,
    }


def format_month(year: Any, month: Any) -> str:
    return f"{int(year):04d}-{int(month):02d}"


def data_quality_warnings(tables: dict[str, pd.DataFrame]) -> list[str]:
    monthly_summary = tables.get("monthly_summary", pd.DataFrame(columns=MONTHLY_SUMMARY_COLUMNS))
    if monthly_summary.empty:
        return []

    warnings = []
    for row in monthly_summary.sort_values(["year", "month"]).to_dict("records"):
        month_label = format_month(row["year"], row["month"])
        amount_total = float(row.get("amount_total") or 0)
        if amount_total > AMOUNT_WARNING_THRESHOLD:
            warnings.append(
                f"{month_label}: suspicious amount_total {amount_total:,.2f} exceeds {AMOUNT_WARNING_THRESHOLD:,.0f}"
            )
        if int(row.get("supplier_count") or 0) == 0 and int(row.get("decision_count") or 0) > 0:
            warnings.append(f"{month_label}: supplier_count is 0")
        decision_count = int(row.get("decision_count") or 0)
        amount_known_count = int(row.get("amount_known_count") or 0)
        if decision_count > 0:
            amount_coverage = amount_known_count / decision_count
            if amount_coverage < LOW_AMOUNT_COVERAGE_THRESHOLD:
                warnings.append(
                    f"{month_label}: low amount coverage {amount_known_count}/{decision_count} "
                    f"({amount_coverage:.0%})"
                )
    return warnings


def print_data_quality_warnings(tables: dict[str, pd.DataFrame]) -> None:
    warnings = data_quality_warnings(tables)
    if not warnings:
        print("Data quality warnings: none")
        return
    print("Data quality warnings:")
    for warning in warnings:
        print(f"- {warning}")


def has_parquet_engine() -> bool:
    for pkg in ("pyarrow", "fastparquet"):
        if importlib.util.find_spec(pkg) is None:
            continue
        try:
            importlib.import_module(pkg)
            return True
        except Exception:
            pass
    return False


def write_tables(
    tables: dict[str, pd.DataFrame], output_root: Path, org: str, output_format: str = "parquet"
) -> dict[str, Path]:
    output_dir = output_root / f"org={org}"
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "decisions": output_dir / f"decisions.{output_format}",
        "suppliers": output_dir / f"suppliers.{output_format}",
        "procurements": output_dir / f"procurements.{output_format}",
        "monthly_summary": output_dir / f"monthly_summary.{output_format}",
    }
    for name, path in paths.items():
        if output_format == "csv":
            tables[name].to_csv(path, index=False)
        else:
            tables[name].to_parquet(path, index=False)
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build normalized tables from cached Diavgeia JSON.")
    parser.add_argument("--org", required=True, help="Diavgeia organizationUid to normalize, e.g. 6166")
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT, help="Raw Diavgeia cache root")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Normalized output root")
    parser.add_argument(
        "--format", choices=OUTPUT_FORMATS, default="parquet", help="Output file format (default: parquet)"
    )
    parser.add_argument(
        "--limit-months",
        type=int,
        default=None,
        help="Only load the first N cached monthly search_export.json files for quick local testing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.format == "parquet" and not has_parquet_engine():
        print(PARQUET_ENGINE_MISSING_MESSAGE)
        return 1

    org_dir = args.raw_root / f"organization_uid={args.org}"
    raw_monthly_files = sorted(org_dir.glob("year=*/month=*/search_export.json")) if org_dir.exists() else []
    if args.limit_months is not None:
        raw_monthly_files = raw_monthly_files[: args.limit_months]
    print(f"Loaded {len(raw_monthly_files)} raw monthly files")

    decisions = load_decisions(args.raw_root, str(args.org), args.limit_months)
    print(f"Parsed {len(decisions)} decision rows")

    tables = build_tables(decisions)
    print_data_quality_warnings(tables)
    print("Starting table writes")
    try:
        paths = write_tables(tables, args.output_root, str(args.org), args.format)
    except ImportError:
        if args.format == "parquet":
            print(PARQUET_ENGINE_MISSING_MESSAGE)
            return 1
        raise
    print("Finished table writes")
    for name, path in paths.items():
        print(f"Wrote {name}: {path} ({len(tables[name])} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
