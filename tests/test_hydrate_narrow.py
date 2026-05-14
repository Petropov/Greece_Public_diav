import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "hydrate_narrow.py"
spec = importlib.util.spec_from_file_location("hydrate_narrow", SCRIPT_PATH)
hydrate_narrow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hydrate_narrow)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class ScoreDecisionTest(unittest.TestCase):
    def test_procurement_type_scores_high(self):
        row = {"decisionTypeUid": "Δ.2.2", "subject": "Contract award"}
        self.assertGreaterEqual(hydrate_narrow.score_decision(row), hydrate_narrow.DEFAULT_MIN_SCORE)

    def test_payroll_subject_scores_very_low(self):
        row = {
            "decisionTypeUid": "Β.2.1",
            "subject": "ΣΥΜΒΑΣΗ ΕΡΓΑΣΙΑΣ ΙΔΙΩΤΙΚΟΥ ΔΙΚΑΙΟΥ ΟΡΙΣΜΕΝΟΥ ΧΡΟΝΟΥ",
        }
        self.assertLess(hydrate_narrow.score_decision(row), 0)

    def test_overtime_pay_scores_very_low(self):
        row = {"subject": "ΑΠΟΖΗΜΙΩΣΗ ΥΠΕΡΩΡΙΑΚΗΣ ΕΡΓΑΣΙΑΣ"}
        self.assertLess(hydrate_narrow.score_decision(row), 0)

    def test_cancellation_scores_very_low(self):
        row = {"subject": "ΑΚΥΡΩΣΗ ΑΝΑΘΕΣΗΣ ΠΡΟΜΗΘΕΙΑΣ"}
        self.assertLess(hydrate_narrow.score_decision(row), 0)

    def test_already_has_amount_and_supplier_reduces_score(self):
        full_row = {
            "decisionTypeUid": "Δ.1",
            "subject": "Ανάθεση προμήθειας υλικών",
            "amountWithVAT": "1000",
            "supplierName": "Acme ΑΕ",
        }
        sparse_row = {
            "decisionTypeUid": "Δ.1",
            "subject": "Ανάθεση προμήθειας υλικών",
        }
        self.assertLess(hydrate_narrow.score_decision(full_row), hydrate_narrow.score_decision(sparse_row))

    def test_procurement_subject_tokens_add_score(self):
        row_no_tokens = {"decisionTypeUid": "Β.1.2", "subject": "something"}
        row_with_tokens = {"decisionTypeUid": "Β.1.2", "subject": "Εκτέλεση σύμβασης προμήθειας υλικών"}
        self.assertGreater(hydrate_narrow.score_decision(row_with_tokens), hydrate_narrow.score_decision(row_no_tokens))


class ParseMonthRangeTest(unittest.TestCase):
    def test_single_month(self):
        result = hydrate_narrow.parse_month_range("2024-03")
        self.assertEqual(result, [(2024, 3)])

    def test_range(self):
        result = hydrate_narrow.parse_month_range("2024-11:2025-02")
        self.assertEqual(result, [(2024, 11), (2024, 12), (2025, 1), (2025, 2)])

    def test_same_start_end(self):
        result = hydrate_narrow.parse_month_range("2025-06:2025-06")
        self.assertEqual(result, [(2025, 6)])


class HydrateMonthTest(unittest.TestCase):
    def _make_search_export(self, root, org, year, month, rows):
        path = hydrate_narrow.search_export_path(root, org, year, month)
        write_json(path, {"decisionResultList": rows})

    def test_skips_already_hydrated(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            org, year, month = "6166", 2026, 3
            self._make_search_export(raw_root, org, year, month, [
                {"ada": "HYDRATED-1", "decisionTypeUid": "Δ.2.2", "subject": "σύμβαση"},
            ])
            existing = hydrate_narrow.decision_cache_path(raw_root, org, year, month, "HYDRATED-1")
            write_json(existing, {"decision": {"ada": "HYDRATED-1"}})

            stats = hydrate_narrow.hydrate_month(
                raw_root, org, year, month,
                min_score=0, dry_run=True, request_delay=0, verbose=False,
            )
            self.assertEqual(stats["already_hydrated"], 1)
            self.assertEqual(stats["fetched"], 0)

    def test_below_threshold_not_fetched(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            org, year, month = "6166", 2026, 3
            self._make_search_export(raw_root, org, year, month, [
                {"ada": "PAYROLL-1", "subject": "ΣΥΜΒΑΣΗ ΕΡΓΑΣΙΑΣ ΙΔΙΩΤΙΚΟΥ ΔΙΚΑΙΟΥ"},
            ])
            stats = hydrate_narrow.hydrate_month(
                raw_root, org, year, month,
                min_score=hydrate_narrow.DEFAULT_MIN_SCORE, dry_run=True, request_delay=0, verbose=False,
            )
            self.assertEqual(stats["below_threshold"], 1)
            self.assertEqual(stats["fetched"], 0)

    def test_procurement_decision_dry_run_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            org, year, month = "6166", 2026, 4
            self._make_search_export(raw_root, org, year, month, [
                {"ada": "PROC-1", "decisionTypeUid": "Δ.2.2", "subject": "Κατακύρωση σύμβασης"},
            ])
            stats = hydrate_narrow.hydrate_month(
                raw_root, org, year, month,
                min_score=hydrate_narrow.DEFAULT_MIN_SCORE, dry_run=True, request_delay=0, verbose=False,
            )
            self.assertEqual(stats["fetched"], 1)

    def test_fetches_and_caches_detail(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            org, year, month = "6166", 2026, 5
            self._make_search_export(raw_root, org, year, month, [
                {"ada": "FETCH-1", "decisionTypeUid": "Δ.2.2", "subject": "Κατακύρωση σύμβασης"},
            ])
            fake_detail = {"decision": {"ada": "FETCH-1", "amountWithVAT": "5000"}}
            with patch.object(hydrate_narrow, "fetch_detail", return_value=fake_detail) as mock_fetch:
                stats = hydrate_narrow.hydrate_month(
                    raw_root, org, year, month,
                    min_score=1, dry_run=False, request_delay=0, verbose=False,
                )
            mock_fetch.assert_called_once_with("FETCH-1")
            self.assertEqual(stats["fetched"], 1)
            self.assertEqual(stats["errors"], 0)
            cache_path = hydrate_narrow.decision_cache_path(raw_root, org, year, month, "FETCH-1")
            self.assertTrue(cache_path.exists())

    def test_error_is_counted_not_raised(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            org, year, month = "6166", 2026, 6
            self._make_search_export(raw_root, org, year, month, [
                {"ada": "ERR-1", "decisionTypeUid": "Δ.2.2", "subject": "Κατακύρωση σύμβασης"},
            ])
            with patch.object(hydrate_narrow, "fetch_detail", side_effect=Exception("network error")):
                stats = hydrate_narrow.hydrate_month(
                    raw_root, org, year, month,
                    min_score=1, dry_run=False, request_delay=0, verbose=False,
                )
            self.assertEqual(stats["errors"], 1)
            self.assertEqual(stats["fetched"], 0)


class FindAllMonthsTest(unittest.TestCase):
    def test_finds_all_cached_months(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            for year, month in [(2024, 11), (2024, 12), (2025, 1)]:
                path = hydrate_narrow.search_export_path(raw_root, "6166", year, month)
                write_json(path, {"decisionResultList": []})
            months = hydrate_narrow.find_all_months(raw_root, "6166")
            self.assertEqual(months, [(2024, 11), (2024, 12), (2025, 1)])


if __name__ == "__main__":
    unittest.main()
