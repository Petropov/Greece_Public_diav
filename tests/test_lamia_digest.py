import unittest
from unittest.mock import patch

from src.lamia_digest import (
    apply_detail_enrichment,
    assign_procurement_groups,
    assert_unique_adas,
    build_top_procurements,
    build_top_suppliers,
    classify_supplier,
    deduplicate_decisions_by_ada,
    extract_amount,
    extract_budget_source,
    extract_supplier_fields,
    fetch_export,
    format_amount,
    has_amount,
    normalize_amount,
    normalize_decision,
    canonical_supplier_name,
    split_malformed_decisions,
    write_json,
    write_markdown,
)


class FakeExportResponse:
    def __init__(self, decisions):
        self._decisions = decisions

    def raise_for_status(self):
        return None

    def json(self):
        return {"decisions": self._decisions}


class FakeExportSession:
    pages = []
    requested_pages = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def get(self, url, params, timeout):
        self.requested_pages.append(params["page"])
        page = params["page"]
        if page < len(self.pages):
            return FakeExportResponse(self.pages[page])
        return FakeExportResponse([])


class DummySession:
    def get(
        self, *args, **kwargs
    ):  # pragma: no cover - should not be used in these tests
        raise AssertionError("metadata endpoint should not be called")


class LamiaDigestAmountTests(unittest.TestCase):
    def test_non_numeric_budget_labels_are_not_amounts(self):
        for value in ("Τακτικός Προϋπολογισμός", "Πρόγραμμα Δημοσίων Επενδύσεων"):
            self.assertIsNone(normalize_amount(value))
            self.assertFalse(has_amount(normalize_amount(value)))
            self.assertEqual(format_amount(value), "—")

    def test_budgettype_is_budget_source_not_amount(self):
        hit = {
            "ada": "TEST",
            "organizationId": "6166",
            "extraFieldValues": {
                "budgettype": "Τακτικός Προϋπολογισμός",
                "amountWithVAT": {"amount": 1234.56, "currency": "EUR"},
            },
        }

        self.assertEqual(
            extract_amount(hit), (1234.56, "extraFieldValues.amountWithVAT.amount")
        )
        self.assertEqual(extract_budget_source(hit), "Τακτικός Προϋπολογισμός")

        normalized = normalize_decision(hit)
        self.assertEqual(normalized["amount"], 1234.56)
        self.assertEqual(normalized["budget_source"], "Τακτικός Προϋπολογισμός")

    def test_supplier_extraction_from_extra_field_values(self):
        hit = {
            "ada": "SUP1",
            "subject": "Ανάθεση προμήθειας",
            "issueDate": "2026-04-01",
            "decisionTypeUid": "Δ.1",
            "extraFieldValues": {
                "sponsorName": "ACME ΕΠΕ",
                "sponsorAFM": "012345678",
                "amountWithVAT": {"amount": "1.000,00", "currency": "EUR"},
            },
        }

        self.assertEqual(
            extract_supplier_fields(hit),
            (
                "ACME ΕΠΕ",
                "012345678",
                "extraFieldValues.sponsorName",
                "extraFieldValues.sponsorAFM",
            ),
        )

        normalized = normalize_decision(hit)
        self.assertEqual(normalized["supplier_name"], "ACME ΕΠΕ")
        self.assertEqual(normalized["supplier_tax_id"], "012345678")
        self.assertEqual(normalized["raw_extra_fields"], hit["extraFieldValues"])

    def test_supplier_extraction_from_labeled_extra_fields(self):
        detail = {
            "extraFieldValues": [
                {"label": "Επωνυμία Αναδόχου", "value": "ΒΗΤΑ ΑΕ"},
                {"label": "ΑΦΜ Αναδόχου", "value": "099999999"},
            ]
        }

        self.assertEqual(
            extract_supplier_fields(detail),
            (
                "ΒΗΤΑ ΑΕ",
                "099999999",
                "extraFieldValues.0.value",
                "extraFieldValues.1.value",
            ),
        )

    def test_markdown_renders_missing_amount_as_dash(self):
        lines = []
        payload = {
            "metadata": {
                "organization_name": "ΔΗΜΟΣ ΛΑΜΙΕΩΝ",
                "organization_uid": "6166",
                "organization_slug": "dhmos_lamieon",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "count": 1,
                "query": "q",
            },
            "decisions": [
                {
                    "decision_date": "2026-04-01",
                    "ada": "TEST",
                    "category": "Other",
                    "decision_type": "Other",
                    "decision_type_raw": "RAW",
                    "decision_type_label": "Other",
                    "title": "Title",
                    "canonical_id": "lamia-decision-test",
                    "duplicate_flag": False,
                    "amount": None,
                    "budget_source": "Πρόγραμμα Δημοσίων Επενδύσεων",
                    "url": "https://example.test",
                }
            ],
        }
        from tempfile import TemporaryDirectory
        from pathlib import Path

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "digest.md"
            write_markdown(path, payload)
            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertIn(
            "| 2026-04-01 | [TEST](https://example.test) | lamia-decision-test | no | Other | Other | RAW | Title | — | — | — | — | Πρόγραμμα Δημοσίων Επενδύσεων | https://example.test |",
            lines,
        )

    def test_markdown_includes_top_supplier_sections(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        payload = {
            "metadata": {
                "organization_name": "ΔΗΜΟΣ ΛΑΜΙΕΩΝ",
                "organization_uid": "6166",
                "organization_slug": "dhmos_lamieon",
                "date_from": "2026-04-01",
                "date_to": "2026-04-30",
                "count": 1,
                "query": "q",
            },
            "top_suppliers_by_amount": [
                {
                    "supplier_name": "ACME ΕΠΕ",
                    "supplier_tax_id": "012345678",
                    "total_amount": 100.0,
                    "amount_decision_count": 1,
                    "decision_count": 1,
                    "adas": ["SUP1"],
                }
            ],
            "top_suppliers_by_count": [
                {
                    "supplier_name": "ACME ΕΠΕ",
                    "supplier_tax_id": "012345678",
                    "total_amount": 100.0,
                    "amount_decision_count": 1,
                    "decision_count": 1,
                    "adas": ["SUP1"],
                }
            ],
            "decisions": [],
        }

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "digest.md"
            write_markdown(path, payload)
            text = path.read_text(encoding="utf-8")

        self.assertIn("## Top Suppliers by Amount", text)
        self.assertIn("## Top Suppliers by Count", text)
        self.assertIn(
            "| 1 | ACME | ACME ΕΠΕ | vendor | tax:012345678 | 012345678 | 100 | 1 | 1 | SUP1 |",
            text,
        )

    def test_supplier_normalization_groups_legal_name_variants(self):
        variants = [
            "Λ. ΚΙΤΣΟΣ Α.Τ.Ε.",
            "ΚΑΤΑΣΚΕΥΕΣ ΤΕΧΝΙΚΩΝ ΕΡΓΩΝ ΛΟΥΚΑΣ ΚΙΤΣΟΣ ΑΝΩΝΥΜΗ ΤΕΧΝΙΚΗ ΕΤΑΙΡΕΙΑ",
        ]

        self.assertEqual(
            [canonical_supplier_name(value) for value in variants],
            ["ΛΟΥΚΑΣ ΚΙΤΣΟΣ", "ΛΟΥΚΑΣ ΚΙΤΣΟΣ"],
        )

        decisions = [
            normalize_decision(
                {
                    "ada": "KIT1",
                    "subject": "Ανάθεση έργου",
                    "issueDate": "2026-04-01",
                    "decisionTypeUid": "Δ.1",
                    "amountWithVAT": "100,00",
                    "supplierName": variants[0],
                }
            ),
            normalize_decision(
                {
                    "ada": "KIT2",
                    "subject": "Ανάθεση έργου",
                    "issueDate": "2026-04-02",
                    "decisionTypeUid": "Δ.1",
                    "amountWithVAT": "200,00",
                    "supplierName": variants[1],
                }
            ),
        ]

        by_amount, by_count = build_top_suppliers(decisions)

        self.assertEqual(by_amount[0]["supplier_normalized_name"], "ΛΟΥΚΑΣ ΚΙΤΣΟΣ")
        self.assertEqual(by_amount[0]["supplier_key"], "name:λουκας κιτσος")
        self.assertEqual(by_amount[0]["decision_count"], 2)
        self.assertEqual(by_amount[0]["total_amount"], 300.0)
        self.assertEqual(by_count[0]["supplier_type"], "vendor")

    def test_supplier_classification_and_filtering_public_internal(self):
        self.assertEqual(classify_supplier("Δ.Ο.Υ. ΛΑΜΙΑΣ"), "tax_authority")
        self.assertEqual(classify_supplier("e-ΕΦΚΑ"), "social_security")
        self.assertEqual(
            classify_supplier("ΣΧΟΛΙΚΗ ΕΠΙΤΡΟΠΗ ΠΡΩΤΟΒΑΘΜΙΑΣ ΕΚΠΑΙΔΕΥΣΗΣ"),
            "school_committee",
        )
        self.assertEqual(classify_supplier("ΔΗΜΟΣ ΛΑΜΙΕΩΝ"), "municipality_internal")
        self.assertEqual(
            classify_supplier("ΠΕΡΙΦΕΡΕΙΑ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ"), "public_authority"
        )

        decisions = [
            normalize_decision(
                {
                    "ada": "TAX",
                    "subject": "Πληρωμή φόρου",
                    "issueDate": "2026-04-01",
                    "decisionTypeUid": "Β.2.1",
                    "amountWithVAT": "1000,00",
                    "supplierName": "Δ.Ο.Υ. ΛΑΜΙΑΣ",
                }
            ),
            normalize_decision(
                {
                    "ada": "VEN",
                    "subject": "Ανάθεση προμήθειας",
                    "issueDate": "2026-04-02",
                    "decisionTypeUid": "Δ.1",
                    "amountWithVAT": "100,00",
                    "supplierName": "ACME ΕΠΕ",
                }
            ),
        ]

        by_amount, _ = build_top_suppliers(
            decisions, exclude_supplier_types=("tax_authority",)
        )

        self.assertEqual(
            [item["supplier_normalized_name"] for item in by_amount], ["ACME"]
        )


class LamiaDigestSignerUnitTests(unittest.TestCase):
    def test_detail_enrichment_maps_direct_signer_and_unit_names(self):
        item = {
            "amount": None,
            "signer": None,
            "unit": None,
            "signer_ids": [],
            "unit_ids": [],
            "organization_id": "6166",
        }
        detail = {
            "amountWithVAT": {"amount": "1.234,50"},
            "signerName": "Signer Name",
            "unitLabel": "Finance Unit",
            "signerIds": ["1"],
            "unitIds": ["2"],
            "extraFieldValues": {
                "budgettype": "Τακτικός Προϋπολογισμός",
                "contractorName": "ΓΑΜΜΑ ΟΕ",
                "contractorAFM": "123456789",
            },
        }

        apply_detail_enrichment(item, detail, DummySession(), 1, {})

        self.assertEqual(item["amount"], 1234.50)
        self.assertEqual(item["signer"], "Signer Name")
        self.assertEqual(item["unit"], "Finance Unit")
        self.assertEqual(item["signer_ids"], ["1"])
        self.assertEqual(item["unit_ids"], ["2"])
        self.assertEqual(item["budget_source"], "Τακτικός Προϋπολογισμός")
        self.assertEqual(item["supplier_name"], "ΓΑΜΜΑ ΟΕ")
        self.assertEqual(item["supplier_tax_id"], "123456789")
        self.assertEqual(item["raw_detail_extra_fields"], detail["extraFieldValues"])

    def test_detail_enrichment_fills_minimum_final_fields(self):
        item = {
            "ada": "DETAIL1",
            "amount": None,
            "signer": None,
            "unit": None,
            "signer_ids": [],
            "unit_ids": [],
            "organization_id": "6166",
            "subject": None,
            "title": None,
            "issue_date": None,
            "decision_date": None,
            "decision_type": None,
            "decision_type_raw": None,
            "missing_subject": True,
        }
        detail = {
            "subject": "Προμήθεια υλικών",
            "issueDate": "2026-04-05T08:30:00",
            "decisionTypeUid": "Δ.1",
            "protocolNumber": "123/2026",
        }

        apply_detail_enrichment(item, detail, DummySession(), 1, {})

        self.assertEqual(item["subject"], "Προμήθεια υλικών")
        self.assertEqual(item["issue_date"], "2026-04-05")
        self.assertEqual(item["decision_date"], "2026-04-05")
        self.assertEqual(item["decision_type"], "Direct procurement assignment")
        self.assertEqual(item["protocol_number"], "123/2026")


class LamiaDigestSemanticQualityTests(unittest.TestCase):
    def test_decision_type_raw_and_normalized_are_preserved(self):
        normalized = normalize_decision(
            {
                "ada": "TYPE1",
                "decisionTypeUid": "Β.2.1",
                "subject": "Έγκριση δαπάνης",
                "issueDate": "2026-04-02T10:00:00",
            }
        )

        self.assertEqual(normalized["decision_type_raw"], "Β.2.1")
        self.assertEqual(normalized["decision_type"], "Payment / expenditure approval")
        self.assertEqual(normalized["title_source"], "subject")
        self.assertFalse(normalized["missing_subject"])

    def test_full_record_enrichment_fills_empty_subject(self):
        item = {
            "amount": None,
            "signer": None,
            "unit": None,
            "signer_ids": [],
            "unit_ids": [],
            "organization_id": "6166",
            "title": None,
            "missing_subject": True,
        }
        detail = {"subject": "Προμήθεια υλικών", "amountWithVAT": {"amount": "50,00"}}

        apply_detail_enrichment(item, detail, DummySession(), 1, {})

        self.assertEqual(item["title"], "Προμήθεια υλικών")
        self.assertEqual(item["title_source"], "full_record:subject")
        self.assertFalse(item["missing_subject"])

    def test_deduplicates_by_ada_for_final_json(self):
        decisions = [
            normalize_decision(
                {
                    "ada": "DUP1",
                    "subject": "Raw subject",
                    "issueDate": "2026-04-01",
                    "decisionTypeUid": "Δ.1",
                }
            ),
            normalize_decision(
                {
                    "ada": "DUP1",
                    "subject": "Duplicate subject",
                    "issueDate": "2026-04-01",
                    "decisionTypeUid": "Δ.1",
                    "protocolNumber": "42",
                }
            ),
            normalize_decision(
                {
                    "ada": "DUP2",
                    "subject": "Other",
                    "issueDate": "2026-04-02",
                    "decisionTypeUid": "Β.2.1",
                }
            ),
        ]

        unique, summary = deduplicate_decisions_by_ada(decisions)

        self.assertEqual([item["ada"] for item in unique], ["DUP1", "DUP2"])
        self.assertEqual(summary["duplicate_ada_rows_removed"], 1)
        self.assertEqual(unique[0]["protocol_number"], "42")
        assert_unique_adas(unique)

    def test_deduplication_keeps_richest_duplicate_row_after_enrichment(self):
        sparse = normalize_decision(
            {
                "ada": "DUP-RICH",
                "issueDate": "2026-04-01",
                "decisionTypeUid": "Δ.1",
            }
        )
        rich = normalize_decision(
            {
                "ada": "DUP-RICH",
                "subject": "Enriched subject",
                "issueDate": "2026-04-01",
                "decisionTypeUid": "Δ.1",
                "protocolNumber": "99/2026",
                "amountWithVAT": "123,45",
            }
        )
        rich["signer"] = "Final Signer"
        rich["unit"] = "Finance Unit"
        rich["enriched_from_full_record"] = True

        unique, summary = deduplicate_decisions_by_ada([sparse, rich])

        self.assertEqual(len(unique), 1)
        self.assertEqual(summary["duplicate_ada_rows_removed"], 1)
        self.assertEqual(unique[0]["title"], "Enriched subject")
        self.assertEqual(unique[0]["amount"], 123.45)
        self.assertEqual(unique[0]["signer"], "Final Signer")
        self.assertTrue(unique[0]["enriched_from_full_record"])
        assert_unique_adas(unique)

    def test_fetch_export_does_not_append_repeated_pages(self):
        FakeExportSession.pages = [
            [{"ada": "ADA1"}, {"ada": "ADA2"}],
            [{"ada": "ADA1"}, {"ada": "ADA2"}],
            [{"ada": "ADA3"}],
        ]
        FakeExportSession.requested_pages = []

        with patch("src.lamia_digest.requests.Session", FakeExportSession):
            decisions = fetch_export("q", limit=10, page_size=2, timeout=1)

        self.assertEqual([item["ada"] for item in decisions], ["ADA1", "ADA2"])
        self.assertEqual(FakeExportSession.requested_pages, [0, 1])

    def test_write_json_asserts_duplicate_ada_entries(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path

        payload = {"decisions": [{"ada": "DUP"}, {"ada": "DUP"}]}
        with TemporaryDirectory() as tmp:
            with self.assertRaises(AssertionError):
                write_json(Path(tmp) / "digest.json", payload)

    def test_malformed_decisions_are_split_from_final_rows(self):
        valid = normalize_decision(
            {
                "ada": "OK",
                "subject": "Subject",
                "issueDate": "2026-04-01",
                "decisionTypeUid": "Δ.1",
            }
        )
        missing_subject = normalize_decision(
            {"ada": "BAD", "issueDate": "2026-04-01", "decisionTypeUid": "Δ.1"}
        )

        rows, malformed = split_malformed_decisions([valid, missing_subject])

        self.assertEqual([item["ada"] for item in rows], ["OK"])
        self.assertEqual(malformed[0]["missing_fields"], ["subject"])

    def test_duplicate_procurements_share_canonical_id(self):
        decisions = [
            normalize_decision(
                {
                    "ada": "ADA1",
                    "decisionTypeUid": "Δ.1",
                    "subject": "Ανάθεση προμήθειας γραφικής ύλης",
                    "issueDate": "2026-04-03T09:00:00",
                    "amountWithVAT": "1.000,00",
                }
            ),
            normalize_decision(
                {
                    "ada": "ADA2",
                    "decisionTypeUid": "Δ.1",
                    "subject": "ΑΝΑΘΕΣΗ ΠΡΟΜΗΘΕΙΑΣ ΓΡΑΦΙΚΗΣ ΥΛΗΣ",
                    "issueDate": "2026-04-03T12:00:00",
                    "amountWithVAT": "1000.00",
                }
            ),
            normalize_decision(
                {
                    "ada": "ADA3",
                    "decisionTypeUid": "Δ.1",
                    "subject": "Ανάθεση υπηρεσιών καθαρισμού",
                    "issueDate": "2026-04-04T12:00:00",
                    "amountWithVAT": "2500.00",
                }
            ),
        ]

        summary = assign_procurement_groups(decisions)
        top = build_top_procurements(decisions)

        self.assertEqual(summary["duplicate_groups"], 1)
        self.assertEqual(decisions[0]["canonical_id"], decisions[1]["canonical_id"])
        self.assertFalse(decisions[0]["duplicate_flag"])
        self.assertTrue(decisions[1]["duplicate_flag"])
        self.assertEqual(top[0]["ada"], "ADA3")
        self.assertEqual(top[1]["duplicate_count"], 1)

    def test_top_suppliers_aggregate_by_tax_id_without_duplicate_adas(self):
        decisions = [
            normalize_decision(
                {
                    "ada": "SUP-DUP",
                    "subject": "Ανάθεση προμήθειας",
                    "issueDate": "2026-04-01",
                    "decisionTypeUid": "Δ.1",
                    "amountWithVAT": "100,00",
                    "supplierName": "ACME ΕΠΕ",
                    "supplierAFM": "012345678",
                }
            ),
            normalize_decision(
                {
                    "ada": "SUP-DUP",
                    "subject": "Ανάθεση προμήθειας duplicate",
                    "issueDate": "2026-04-01",
                    "decisionTypeUid": "Δ.1",
                    "amountWithVAT": "100,00",
                    "supplierName": "ACME ΕΠΕ",
                    "supplierAFM": "012345678",
                    "protocolNumber": "77",
                }
            ),
            normalize_decision(
                {
                    "ada": "SUP2",
                    "subject": "Ανάθεση υπηρεσιών",
                    "issueDate": "2026-04-02",
                    "decisionTypeUid": "Δ.1",
                    "amountWithVAT": "250,00",
                    "supplierName": "ΒΗΤΑ ΑΕ",
                    "supplierAFM": "099999999",
                }
            ),
        ]

        unique, _ = deduplicate_decisions_by_ada(decisions)
        assert_unique_adas(unique)
        by_amount, by_count = build_top_suppliers(unique)

        self.assertEqual([item["ada"] for item in unique], ["SUP-DUP", "SUP2"])
        self.assertEqual(by_amount[0]["supplier_name"], "ΒΗΤΑ ΑΕ")
        self.assertEqual(by_amount[0]["total_amount"], 250.0)
        self.assertEqual(by_count[0]["decision_count"], 1)
        self.assertEqual(by_count[0]["adas"], ["SUP2"])


if __name__ == "__main__":
    unittest.main()
