import importlib.util
import csv
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "cluster_suppliers.py"
spec = importlib.util.spec_from_file_location("cluster_suppliers", SCRIPT_PATH)
cluster_suppliers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cluster_suppliers)


def make_supplier(supplier_key, name, tax_id, decision_count=1, total_amount=1000.0, first_seen="2024-01-01", last_seen="2024-12-31"):
    return {
        "supplier_key": supplier_key,
        "supplier_name_normalized": name,
        "supplier_tax_id": tax_id,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "decision_count": str(decision_count),
        "total_amount": str(total_amount),
    }


class BuildClustersTest(unittest.TestCase):
    def test_same_tax_id_merges_into_one_cluster(self):
        rows = [
            make_supplier("tax:123456789", "ACME ΑΕ", "123456789", 3, 5000.0),
            make_supplier("name:abc123", "ACME SUPPLIES", "123456789", 2, 3000.0),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0]["supplier_tax_id"], "123456789")
        self.assertEqual(clusters[0]["decision_count"], 5)
        self.assertAlmostEqual(float(clusters[0]["total_amount"]), 8000.0)

    def test_same_canonical_name_no_tax_id_merges(self):
        rows = [
            make_supplier("name:aaa111", "ΚΑΘΑΡΙΣΜΟΣ ΑΕ", "", 2, 2000.0),
            make_supplier("name:bbb222", "ΚΑΘΑΡΙΣΜΟΣ ΑΕ", "", 1, 1500.0),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0]["decision_count"], 3)

    def test_different_tax_ids_remain_separate(self):
        rows = [
            make_supplier("tax:111111111", "SUPPLIER A", "111111111"),
            make_supplier("tax:222222222", "SUPPLIER B", "222222222"),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        self.assertEqual(len(clusters), 2)

    def test_different_names_no_tax_id_remain_separate(self):
        rows = [
            make_supplier("name:aaa", "ΕΤΑΙΡΕΙΑ ΑΛΦΑ", "", 1, 100.0),
            make_supplier("name:bbb", "ΕΤΑΙΡΕΙΑ ΒΗΤΑ", "", 1, 200.0),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        self.assertEqual(len(clusters), 2)

    def test_sorted_by_total_amount_descending(self):
        rows = [
            make_supplier("tax:111", "A", "111111111", 1, 500.0),
            make_supplier("tax:222", "B", "222222222", 1, 5000.0),
            make_supplier("tax:333", "C", "333333333", 1, 100.0),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        amounts = [float(c["total_amount"]) for c in clusters]
        self.assertEqual(amounts, sorted(amounts, reverse=True))

    def test_member_keys_joined_with_pipe(self):
        rows = [
            make_supplier("tax:123456789", "ACME", "123456789"),
            make_supplier("name:zzz999", "ACME SUPPLIES", "123456789"),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        member_keys = set(clusters[0]["member_keys"].split("|"))
        self.assertEqual(member_keys, {"tax:123456789", "name:zzz999"})

    def test_el_prefix_tax_id_normalized(self):
        rows = [
            make_supplier("name:aaa", "OMEGA", "EL123456789"),
        ]
        clusters = cluster_suppliers.build_clusters(rows)
        self.assertEqual(clusters[0]["supplier_tax_id"], "123456789")

    def test_empty_input_returns_empty(self):
        self.assertEqual(cluster_suppliers.build_clusters([]), [])


class WriteReadClustersTest(unittest.TestCase):
    def test_write_and_read_roundtrip(self):
        clusters = [
            {
                "cluster_id": "cluster:abc",
                "canonical_name": "TEST SUPPLIER",
                "supplier_tax_id": "123456789",
                "member_keys": "tax:123456789",
                "decision_count": 5,
                "total_amount": 9999.99,
                "first_seen": "2023-01-01",
                "last_seen": "2024-12-31",
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "clusters.csv"
            cluster_suppliers.write_clusters_csv(out_path, clusters)
            rows = cluster_suppliers.read_suppliers_csv(out_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["cluster_id"], "cluster:abc")
        self.assertAlmostEqual(float(rows[0]["total_amount"]), 9999.99)


if __name__ == "__main__":
    unittest.main()
