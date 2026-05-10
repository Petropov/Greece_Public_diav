import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

HAS_PARQUET_ENGINE = bool(importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"))

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_normalized_tables.py"
spec = importlib.util.spec_from_file_location("build_normalized_tables", SCRIPT_PATH)
build_normalized_tables = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_normalized_tables)


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


if __name__ == "__main__":
    unittest.main()
