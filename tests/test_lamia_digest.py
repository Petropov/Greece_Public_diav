import unittest

from src.lamia_digest import (
    apply_detail_enrichment,
    extract_amount,
    extract_budget_source,
    format_amount,
    has_amount,
    normalize_amount,
    normalize_decision,
    write_markdown,
)


class DummySession:
    def get(self, *args, **kwargs):  # pragma: no cover - should not be used in these tests
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

        self.assertEqual(extract_amount(hit), (1234.56, "extraFieldValues.amountWithVAT.amount"))
        self.assertEqual(extract_budget_source(hit), "Τακτικός Προϋπολογισμός")

        normalized = normalize_decision(hit)
        self.assertEqual(normalized["amount"], 1234.56)
        self.assertEqual(normalized["budget_source"], "Τακτικός Προϋπολογισμός")

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
                    "decision_type_label": "Other",
                    "title": "Title",
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

        self.assertIn("| 2026-04-01 | [TEST](https://example.test) | Other | Other | Title | — | — | Πρόγραμμα Δημοσίων Επενδύσεων | https://example.test |", lines)


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
            "extraFieldValues": {"budgettype": "Τακτικός Προϋπολογισμός"},
        }

        apply_detail_enrichment(item, detail, DummySession(), 1, {})

        self.assertEqual(item["amount"], 1234.50)
        self.assertEqual(item["signer"], "Signer Name")
        self.assertEqual(item["unit"], "Finance Unit")
        self.assertEqual(item["signer_ids"], ["1"])
        self.assertEqual(item["unit_ids"], ["2"])
        self.assertEqual(item["budget_source"], "Τακτικός Προϋπολογισμός")


if __name__ == "__main__":
    unittest.main()
