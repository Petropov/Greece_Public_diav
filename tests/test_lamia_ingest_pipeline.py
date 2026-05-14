import importlib.util
import json
import tempfile
import sys
from datetime import date
from pathlib import Path
import unittest
from unittest.mock import Mock

import pandas as pd
import requests

from src.lamia_digest import extract_amount, extract_supplier_fields, normalize_decision

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "ingest_diavgeia.py"
spec = importlib.util.spec_from_file_location("ingest_diavgeia", SCRIPT_PATH)
ingest = importlib.util.module_from_spec(spec)
sys.modules["ingest_diavgeia"] = ingest
spec.loader.exec_module(ingest)


class LamiaIngestPipelineTest(unittest.TestCase):
    def test_monthly_date_slicing(self):
        months = list(ingest.iter_months(date(2019, 1, 1), date(2019, 3, 1)))
        self.assertEqual(months, [date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1)])
        self.assertEqual(ingest.month_bounds(date(2020, 2, 1)), (date(2020, 2, 1), date(2020, 2, 29)))

    def test_pagination_loop_continues_beyond_500_rows(self):
        calls = []

        def fake_request_json(session, url, *, params, timeout, max_retries, sleep):
            calls.append(params["page"])
            size = params["size"]
            if params["page"] == 0:
                return {"decisionResultList": [{"ada": f"A{i}", "issueDate": "2026-01-01"} for i in range(size)]}, 1
            if params["page"] == 1:
                return {"decisionResultList": [{"ada": f"B{i}", "issueDate": "2026-01-02"} for i in range(25)]}, 1
            return {"decisionResultList": []}, 1

        original = ingest.request_json
        ingest.request_json = fake_request_json
        try:
            rows, audit = ingest.fetch_month_index(requests.Session(), "6166", date(2026, 1, 1), size=500, timeout=1, max_retries=1, sleep=0)
        finally:
            ingest.request_json = original

        self.assertEqual(calls, [0, 1])
        self.assertEqual(len(rows), 525)
        self.assertTrue(audit.exact_500_flag)
        self.assertTrue(audit.pagination_complete)

    def test_european_amount_parsing_from_subject(self):
        amount, source = extract_amount({"subject": "Έγκριση σύμβασης ποσού 2.002.600,00€ για έργο"})
        self.assertEqual(amount, 2002600.0)
        self.assertEqual(source, "subject.text")

    def test_supplier_parsing_from_person_array(self):
        name, afm, name_source, afm_source = extract_supplier_fields(
            {"extraFieldValues": {"person": [{"firstName": "ΙΩΑΝΝΗΣ", "lastName": "ΠΑΠΑΔΟΠΟΥΛΟΣ", "afm": "123456789"}]}}
        )
        self.assertEqual(name, "ΙΩΑΝΝΗΣ ΠΑΠΑΔΟΠΟΥΛΟΣ")
        self.assertEqual(afm, "123456789")
        self.assertIn("person", name_source)
        self.assertIn("afm", afm_source)

    def test_supplier_parsing_from_sponsor_afm_name(self):
        name, afm, name_source, afm_source = extract_supplier_fields(
            {"extraFieldValues": {"sponsor": [{"sponsorAFMName": "ACME ΑΕ", "sponsorAFM": "EL123456789"}]}}
        )
        self.assertEqual(name, "ACME ΑΕ")
        self.assertEqual(afm, "123456789")
        self.assertIn("sponsorAFMName", name_source)
        self.assertIn("sponsorAFM", afm_source)

    def test_cache_hit_avoids_api_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp)
            path = ingest.cache_path(raw_root, "6166", "ABC-123")
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"decision": {"ada": "ABC-123"}}), encoding="utf-8")
            session = Mock()
            payload, attempts, status = ingest.load_or_fetch_detail(
                session, raw_root, "6166", "ABC-123", force_refresh=False, timeout=1, max_retries=1, sleep=0
            )
            self.assertEqual(payload["decision"]["ada"], "ABC-123")
            self.assertEqual(attempts, 0)
            self.assertEqual(status, "cache")
            session.get.assert_not_called()

    def test_confidence_rating_logic(self):
        self.assertEqual(ingest.confidence_rating(pagination_complete=True, exact_500_flag=False, hydration_pct=90, amount_pct=70, supplier_pct=70), "green")
        self.assertEqual(ingest.confidence_rating(pagination_complete=True, exact_500_flag=False, hydration_pct=50, amount_pct=70, supplier_pct=70), "yellow")
        self.assertEqual(ingest.confidence_rating(pagination_complete=True, exact_500_flag=True, hydration_pct=90, amount_pct=70, supplier_pct=70), "red")

    def test_nested_amount_and_supplier_provenance_in_normalize_decision(self):
        row = normalize_decision({
            "ada": "NEST",
            "subject": "Ανάθεση",
            "extraFieldValues": {
                "awardAmount": {"amount": "1.234,56", "currency": "EUR"},
                "sponsor": [{"expenseAmount": {"amount": "500,00", "currency": "EUR"}, "sponsorAFMName": "ACME", "sponsorAFM": "123456789"}],
            },
        })
        self.assertEqual(row["amount"], 1234.56)
        self.assertIn("awardAmount.amount", row["amount_source"])
        self.assertEqual(row["supplier_name"], "ACME")
        self.assertEqual(row["supplier_tax_id"], "123456789")


if __name__ == "__main__":
    unittest.main()
