import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_normalized_tables.py"
spec = importlib.util.spec_from_file_location("build_normalized_tables", SCRIPT_PATH)
build_normalized_tables = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_normalized_tables)

HAS_PARQUET_ENGINE = build_normalized_tables.has_parquet_engine()


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class BuildNormalizedTablesTest(unittest.TestCase):
    def test_builds_tables_from_search_and_detail_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            output_root = Path(tmp) / "normalized"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=04"
            write_json(
                month_dir / "search_export.json",
                {
                    "decisionResultList": [
                        {
                            "ada": "ADA-1",
                            "issueDate": "01/04/2026 00:00:00",
                            "decisionTypeUid": "Δ.1",
                            "subject": "Initial assignment subject",
                        },
                        {
                            "ada": "ADA-2",
                            "issueDate": "2026-04-02",
                            "decisionTypeUid": "Α.1",
                            "subject": "Non-financial regulatory decision",
                        },
                    ]
                },
            )
            write_json(
                month_dir / "decisions" / "ADA-1.json",
                {
                    "decision": {
                        "ada": "ADA-1",
                        "subject": "Ανάθεση προμήθειας υλικών",
                        "amountWithVAT": "1.234,56",
                        "documentUrl": "https://example.test/ADA-1.pdf",
                        "extraFieldValues": [
                            {"label": "Ανάδοχος", "value": "Acme Supplies ΑΕ"},
                            {"label": "ΑΦΜ Αναδόχου", "value": "EL123456789"},
                        ],
                        "signerName": {"firstName": "Maria", "lastName": "Signer"},
                        "unitName": "Procurement Unit",
                    }
                },
            )

            decisions = build_normalized_tables.load_decisions(raw_root, "6166")
            tables = build_normalized_tables.build_tables(decisions)
            decisions_df = tables["decisions"]
            self.assertEqual(len(decisions_df), 2)
            first = decisions_df.set_index("ada").loc["ADA-1"]
            self.assertEqual(first["org"], "6166")
            self.assertEqual(first["year"], 2026)
            self.assertEqual(first["month"], 4)
            self.assertEqual(first["issue_date"], "2026-04-01")
            self.assertEqual(first["decision_type"], "Procurement assignment")
            self.assertEqual(first["subject"], "Ανάθεση προμήθειας υλικών")
            self.assertEqual(first["url"], "https://example.test/ADA-1.pdf")
            self.assertAlmostEqual(first["amount"], 1234.56)
            self.assertEqual(first["amount_source"], "detail:amountWithVAT")
            self.assertEqual(first["supplier_name"], "Acme Supplies ΑΕ")
            self.assertEqual(first["supplier_tax_id"], "123456789")
            self.assertEqual(first["signer"], "Maria Signer")
            self.assertEqual(first["unit"], "Procurement Unit")

            suppliers_df = tables["suppliers"]
            self.assertEqual(len(suppliers_df), 1)
            supplier = suppliers_df.iloc[0]
            self.assertEqual(supplier["supplier_key"], "tax:123456789")
            self.assertEqual(supplier["supplier_name_normalized"], "ACME SUPPLIES ΑΕ")
            self.assertEqual(supplier["first_seen"], "2026-04-01")
            self.assertEqual(supplier["last_seen"], "2026-04-01")
            self.assertEqual(supplier["decision_count"], 1)
            self.assertAlmostEqual(supplier["total_amount"], 1234.56)

            procurements_df = tables["procurements"]
            self.assertEqual(list(procurements_df["ada"]), ["ADA-1"])
            self.assertEqual(procurements_df.iloc[0]["supplier_key"], "tax:123456789")

            monthly_df = tables["monthly_summary"]
            self.assertEqual(len(monthly_df), 1)
            summary = monthly_df.iloc[0]
            self.assertEqual(summary["year"], 2026)
            self.assertEqual(summary["month"], 4)
            self.assertEqual(summary["decision_count"], 2)
            self.assertAlmostEqual(summary["amount_total"], 1234.56)
            self.assertEqual(summary["supplier_count"], 1)
            self.assertEqual(summary["amount_known_count"], 1)
            self.assertEqual(summary["amount_missing_count"], 1)
            self.assertEqual(summary["supplier_known_count"], 1)
            self.assertEqual(summary["supplier_missing_count"], 1)
            self.assertEqual(summary["detail_enriched_decision_count"], 1)
            self.assertEqual(summary["search_only_decision_count"], 1)

    def test_text_numbers_in_subjects_are_not_parsed_as_amounts(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=01"
            write_json(
                month_dir / "search_export.json",
                {
                    "decisionResultList": [
                        {
                            "ada": "TEXT-NUMBER-1",
                            "issueDate": "2026-01-03",
                            "subject": "Έγκριση πρακτικού 784963597095 για προμήθεια υλικών",
                        }
                    ]
                },
            )

            tables = build_normalized_tables.build_tables(
                build_normalized_tables.load_decisions(raw_root, "6166")
            )

            decision = tables["decisions"].iloc[0]
            self.assertTrue(pd.isna(decision["amount"]))
            self.assertIsNone(decision["amount_source"])
            summary = tables["monthly_summary"].iloc[0]
            self.assertEqual(summary["amount_known_count"], 0)
            self.assertEqual(summary["amount_missing_count"], 1)

    def test_nested_subject_numbers_are_not_parsed_as_amounts(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=01"
            write_json(
                month_dir / "search_export.json",
                {
                    "decisionResultList": [
                        {
                            "ada": "NESTED-SUBJECT-NUMBER-1",
                            "issueDate": "2026-01-04",
                            "subject": {
                                "title": "Έγκριση πρακτικού 784963597095",
                                "amount": "784963597095",
                            },
                        }
                    ]
                },
            )

            tables = build_normalized_tables.build_tables(
                build_normalized_tables.load_decisions(raw_root, "6166")
            )

            decision = tables["decisions"].iloc[0]
            self.assertTrue(pd.isna(decision["amount"]))
            self.assertIsNone(decision["amount_source"])

    def test_structured_amount_fields_are_parsed_correctly(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=02"
            write_json(
                month_dir / "search_export.json",
                {
                    "decisionResultList": [
                        {
                            "ada": "STRUCTURED-AMOUNT-1",
                            "issueDate": "2026-02-03",
                            "subject": "Προμήθεια με δομημένο ποσό",
                            "amountWithVAT": "2.345,67",
                        }
                    ]
                },
            )

            tables = build_normalized_tables.build_tables(
                build_normalized_tables.load_decisions(raw_root, "6166")
            )

            decision = tables["decisions"].iloc[0]
            self.assertAlmostEqual(decision["amount"], 2345.67)
            self.assertEqual(decision["amount_source"], "search_export:amountWithVAT")
            self.assertAlmostEqual(tables["monthly_summary"].iloc[0]["amount_total"], 2345.67)

    def test_monthly_summary_coverage_fields_are_populated(self):
        tables = build_normalized_tables.build_tables(
            [
                {
                    "org": "6166",
                    "year": 2026,
                    "month": 3,
                    "ada": "KNOWN-1",
                    "issue_date": "2026-03-01",
                    "decision_type": "Procurement assignment",
                    "subject": "Προμήθεια",
                    "url": None,
                    "amount": 100.0,
                    "amount_source": "detail:amount",
                    "supplier_name": "Known Supplier",
                    "supplier_tax_id": "123456789",
                    "signer": None,
                    "unit": None,
                    "_detail_enriched": True,
                },
                {
                    "org": "6166",
                    "year": 2026,
                    "month": 3,
                    "ada": "MISSING-1",
                    "issue_date": "2026-03-02",
                    "decision_type": "Regulatory act",
                    "subject": "Χωρίς ποσό ή ανάδοχο",
                    "url": None,
                    "amount": None,
                    "amount_source": None,
                    "supplier_name": None,
                    "supplier_tax_id": None,
                    "signer": None,
                    "unit": None,
                    "_detail_enriched": False,
                },
            ]
        )

        summary = tables["monthly_summary"].iloc[0]
        self.assertEqual(summary["amount_known_count"], 1)
        self.assertEqual(summary["amount_missing_count"], 1)
        self.assertEqual(summary["supplier_known_count"], 1)
        self.assertEqual(summary["supplier_missing_count"], 1)
        self.assertEqual(summary["detail_enriched_decision_count"], 1)
        self.assertEqual(summary["search_only_decision_count"], 1)

    def test_data_quality_warnings_include_suspicious_totals_missing_suppliers_and_low_coverage(self):
        tables = {
            "monthly_summary": pd.DataFrame(
                [
                    {
                        "year": 2026,
                        "month": 4,
                        "decision_count": 3,
                        "amount_total": 10_000_001.0,
                        "supplier_count": 0,
                        "amount_known_count": 1,
                        "amount_missing_count": 2,
                        "supplier_known_count": 0,
                        "supplier_missing_count": 3,
                        "detail_enriched_decision_count": 0,
                        "search_only_decision_count": 3,
                    }
                ]
            )
        }

        warnings = build_normalized_tables.data_quality_warnings(tables)

        self.assertTrue(any("suspicious amount_total" in warning for warning in warnings))
        self.assertTrue(any("supplier_count is 0" in warning for warning in warnings))
        self.assertTrue(any("low amount coverage" in warning for warning in warnings))

    def test_procurement_classification_uses_canonical_tokens(self):
        decision = {
            "decision_type": "Regulatory act",
            "subject": "Σύμβαση καθαρισμού χωρίς ποσό",
            "supplier_name": None,
            "supplier_tax_id": None,
            "amount": None,
        }

        self.assertTrue(build_normalized_tables.is_procurement(decision))

    def test_procurement_classification_uses_precomputed_searchable_text(self):
        decision = {
            "_procurement_searchable_text": build_normalized_tables.procurement_searchable_text(
                {"decision_type": "Regulatory act", "subject": "Προμήθεια υλικών"}
            ),
            "supplier_name": None,
            "supplier_tax_id": None,
            "amount": None,
        }

        with patch.object(
            build_normalized_tables,
            "canonical_text",
            side_effect=AssertionError("canonical_text should not be called"),
        ):
            self.assertTrue(build_normalized_tables.is_procurement(decision))

    def test_limit_months_loads_first_cached_months_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            april_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=04"
            may_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=05"
            write_json(april_dir / "search_export.json", {"decisionResultList": [{"ada": "APRIL-1"}]})
            write_json(may_dir / "search_export.json", {"decisionResultList": [{"ada": "MAY-1"}]})

            decisions = build_normalized_tables.load_decisions(raw_root, "6166", limit_months=1)

            self.assertEqual([decision["ada"] for decision in decisions], ["APRIL-1"])

    def test_write_tables_csv_does_not_require_parquet_engine(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            output_root = Path(tmp) / "normalized"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=05"
            write_json(
                month_dir / "search_export.json",
                {"decisionResultList": [{"ada": "CSV-1", "issueDate": "2026-05-03", "amount": 10}]},
            )
            tables = build_normalized_tables.build_tables(
                build_normalized_tables.load_decisions(raw_root, "6166")
            )

            with patch.object(
                pd.DataFrame, "to_parquet", side_effect=AssertionError("parquet should not be used")
            ):
                paths = build_normalized_tables.write_tables(tables, output_root, "6166", "csv")

            self.assertEqual(paths["decisions"], output_root / "org=6166" / "decisions.csv")
            self.assertTrue((output_root / "org=6166" / "decisions.csv").exists())
            self.assertTrue((output_root / "org=6166" / "suppliers.csv").exists())
            self.assertTrue((output_root / "org=6166" / "procurements.csv").exists())
            self.assertTrue((output_root / "org=6166" / "monthly_summary.csv").exists())
            decisions_df = pd.read_csv(output_root / "org=6166" / "decisions.csv")
            self.assertEqual(len(decisions_df), 1)
            self.assertEqual(decisions_df.iloc[0]["ada"], "CSV-1")
            self.assertEqual(decisions_df.iloc[0]["amount"], 10)

    def test_cli_writes_csv_org_partition_without_parquet_engine(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            output_root = Path(tmp) / "normalized"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=05"
            write_json(
                month_dir / "search_export.json",
                {"decisionResultList": [{"ada": "CLI-CSV-1", "issueDate": "2026-05-03", "amount": 10}]},
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--org",
                    "6166",
                    "--raw-root",
                    str(raw_root),
                    "--output-root",
                    str(output_root),
                    "--format",
                    "csv",
                ],
                check=True,
                text=True,
                capture_output=True,
            )

            self.assertIn("Wrote decisions", result.stdout)
            self.assertTrue((output_root / "org=6166" / "decisions.csv").exists())
            self.assertFalse((output_root / "org=6166" / "decisions.parquet").exists())
            self.assertEqual(len(pd.read_csv(output_root / "org=6166" / "decisions.csv")), 1)

    def test_parquet_missing_message_is_clear_without_traceback(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            output_root = Path(tmp) / "normalized"
            argv = [
                "build_normalized_tables.py",
                "--org",
                "6166",
                "--raw-root",
                str(raw_root),
                "--output-root",
                str(output_root),
            ]
            with patch.object(sys, "argv", argv), patch.object(
                build_normalized_tables, "has_parquet_engine", return_value=False
            ):
                with patch("builtins.print") as mocked_print:
                    exit_code = build_normalized_tables.main()

            self.assertEqual(exit_code, 1)
            mocked_print.assert_called_once_with(build_normalized_tables.PARQUET_ENGINE_MISSING_MESSAGE)
            self.assertFalse((output_root / "org=6166").exists())

    @unittest.skipUnless(HAS_PARQUET_ENGINE, "pandas parquet engine is not installed")
    def test_cli_writes_expected_org_partition_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_root = Path(tmp) / "raw"
            output_root = Path(tmp) / "normalized"
            month_dir = raw_root / "organization_uid=6166" / "year=2026" / "month=05"
            write_json(
                month_dir / "search_export.json",
                {"decisionResultList": [{"ada": "CLI-1", "issueDate": "2026-05-03", "amount": 10}]},
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--org",
                    "6166",
                    "--raw-root",
                    str(raw_root),
                    "--output-root",
                    str(output_root),
                ],
                check=True,
                text=True,
                capture_output=True,
            )

            self.assertIn("Wrote decisions", result.stdout)
            self.assertTrue((output_root / "org=6166" / "decisions.parquet").exists())
            self.assertEqual(len(pd.read_parquet(output_root / "org=6166" / "decisions.parquet")), 1)


class PayrollAdminNoiseTest(unittest.TestCase):
    def _decision(self, subject, decision_type=None, supplier=None, amount=None):
        return {
            "decision_type": decision_type or "Expenditure approval",
            "subject": subject,
            "supplier_name": supplier,
            "supplier_tax_id": None,
            "amount": amount,
        }

    def test_employment_contract_is_excluded(self):
        d = self._decision("ΣΥΜΒΑΣΗ ΕΡΓΑΣΙΑΣ ΙΔΙΩΤΙΚΟΥ ΔΙΚΑΙΟΥ ΟΡΙΣΜΕΝΟΥ ΧΡΟΝΟΥ")
        self.assertFalse(build_normalized_tables.is_procurement(d))

    def test_overtime_pay_is_excluded(self):
        d = self._decision("ΑΠΟΖΗΜΙΩΣΗ ΥΠΕΡΩΡΙΑΚΗΣ ΕΡΓΑΣΙΑΣ ΜΟΝΙΜΟΥ ΥΠΑΛΛΗΛΟΥ")
        self.assertFalse(build_normalized_tables.is_procurement(d))

    def test_payroll_ratification_is_excluded(self):
        d = self._decision("ΚΥΡΩΣΗ ΜΙΣΘΟΔΟΤΙΚΗΣ ΚΑΤΑΣΤΑΣΗΣ ΙΟΥΛΙΟΥ")
        self.assertFalse(build_normalized_tables.is_procurement(d))

    def test_job_vacancy_is_excluded(self):
        d = self._decision("ΠΡΟΚΗΡΥΞΗ ΠΛΗΡΩΣΗΣ ΘΕΣΕΩΝ ΜΟΝΙΜΟΥ ΠΡΟΣΩΠΙΚΟΥ")
        self.assertFalse(build_normalized_tables.is_procurement(d))

    def test_oath_taking_is_excluded(self):
        d = self._decision("ΟΡΚΩΜΟΣΙΑ ΝΕΩΝ ΔΗΜΟΤΙΚΩΝ ΣΥΜΒΟΥΛΩΝ")
        self.assertFalse(build_normalized_tables.is_procurement(d))

    def test_leave_grant_is_excluded(self):
        d = self._decision("ΧΟΡΗΓΗΣΗ ΑΔΕΙΑΣ ΑΠΟΥΣΙΑΣ ΥΠΑΛΛΗΛΟΥ")
        self.assertFalse(build_normalized_tables.is_procurement(d))

    def test_normal_procurement_not_excluded(self):
        d = self._decision("ΑΝΑΘΕΣΗ ΠΡΟΜΗΘΕΙΑΣ ΥΛΙΚΩΝ ΚΑΘΑΡΙΟΤΗΤΑΣ", amount=5000.0)
        self.assertTrue(build_normalized_tables.is_procurement(d))

    def test_katakyrosi_award_not_confused_with_akyrosi_cancellation(self):
        """κατακύρωση (award) must not match the ακύρωση (cancellation) substring filter."""
        d = self._decision("ΚΑΤΑΚΥΡΩΣΗ ΑΠΟΤΕΛΕΣΜΑΤΩΝ ΔΙΑΓΩΝΙΣΜΟΥ", amount=15000.0)
        self.assertTrue(build_normalized_tables.is_procurement(d))

    def test_publication_costs_for_hiring_not_excluded(self):
        """Publication expenses for job announcements are legitimate procurement costs."""
        d = self._decision(
            "ΕΞΟΔΑ ΔΗΜΟΣΙΕΥΣΗΣ ΑΝΑΚΟΙΝΩΣΗΣ ΓΙΑ ΠΡΟΣΛΗΨΗ ΠΡΟΣΩΠΙΚΟΥ",
            amount=200.0,
        )
        self.assertTrue(build_normalized_tables.is_procurement(d))

    def test_payroll_admin_tokens_constant_exported(self):
        self.assertTrue(len(build_normalized_tables.PAYROLL_ADMIN_TOKENS) > 0)
        self.assertIn("μισθοδοσια", build_normalized_tables.PAYROLL_ADMIN_TOKENS)


if __name__ == "__main__":
    unittest.main()
