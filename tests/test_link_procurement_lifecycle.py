"""Tests for scripts/link_procurement_lifecycle.py — procurement lifecycle linker."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importlib
lpl = importlib.import_module("scripts.link_procurement_lifecycle")


def make_row(**kwargs):
    defaults = {
        "ada": "TEST001",
        "decision_type": "ΚΑΤΑΚΥΡΩΣΗ",
        "amount": "100000",
        "issue_date": "2024-06-15",
        "supplier_tax_id": "123456789",
        "supplier_key": "tax:123456789",
        "subject": "Test procurement",
        "supplier_name_raw": "TEST SUPPLIER",
    }
    defaults.update(kwargs)
    return defaults


class TestAmountsMatch:
    def test_identical_amounts(self):
        assert lpl.amounts_match(100_000, 100_000)

    def test_within_tolerance(self):
        assert lpl.amounts_match(100_000, 101_999)  # < 2%

    def test_just_outside_tolerance(self):
        assert not lpl.amounts_match(100_000, 103_000)  # > 2%

    def test_zero_amounts(self):
        assert not lpl.amounts_match(0, 0)
        assert not lpl.amounts_match(0, 100_000)

    def test_rounding_vat(self):
        # Same contract, VAT applied slightly differently
        assert lpl.amounts_match(2_132_543, 2_132_543.00)
        assert lpl.amounts_match(2_132_543, 2_134_000)  # ~0.07%


class TestDatesWithin:
    def test_same_date(self):
        from datetime import datetime
        d = datetime(2024, 6, 1)
        assert lpl.dates_within(d, d)

    def test_within_window(self):
        from datetime import datetime
        assert lpl.dates_within(datetime(2024, 1, 1), datetime(2024, 6, 1))  # 152 days

    def test_outside_window(self):
        from datetime import datetime
        assert not lpl.dates_within(datetime(2023, 1, 1), datetime(2024, 6, 1))  # > 180

    def test_none_is_permissive(self):
        from datetime import datetime
        assert lpl.dates_within(None, datetime(2024, 1, 1))
        assert lpl.dates_within(None, None)


class TestLinkLifecycle:
    def test_single_row_no_amount_passthrough(self):
        rows = [make_row(amount="0")]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 1
        assert contracts[0]["stage_count"] == 1

    def test_single_row_with_amount(self):
        rows = [make_row(amount="100000")]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 1
        assert lpl.safe_float(contracts[0]["amount"]) == 100_000

    def test_two_matching_rows_grouped(self):
        rows = [
            make_row(ada="ADA001", decision_type="ΚΑΤΑΚΥΡΩΣΗ", amount="100000", issue_date="2024-06-01"),
            make_row(ada="ADA002", decision_type="ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ", amount="100000", issue_date="2024-05-15"),
        ]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 1
        c = contracts[0]
        assert c["stage_count"] == 2
        assert "ADA001" in c["all_adas"]
        assert "ADA002" in c["all_adas"]

    def test_different_amounts_not_grouped(self):
        rows = [
            make_row(ada="ADA001", amount="100000", issue_date="2024-06-01"),
            make_row(ada="ADA002", amount="200000", issue_date="2024-06-02"),
        ]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 2

    def test_different_suppliers_not_grouped(self):
        rows = [
            make_row(ada="ADA001", amount="100000", supplier_tax_id="111111111"),
            make_row(ada="ADA002", amount="100000", supplier_tax_id="222222222"),
        ]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 2

    def test_prefers_katakyrosi_over_kanonistiki(self):
        rows = [
            make_row(ada="ADA001", decision_type="ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ", amount="100000", issue_date="2024-05-01"),
            make_row(ada="ADA002", decision_type="ΚΑΤΑΚΥΡΩΣΗ", amount="100000", issue_date="2024-06-01"),
        ]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 1
        assert contracts[0]["decision_type"] == "ΚΑΤΑΚΥΡΩΣΗ"
        assert contracts[0]["ada"] == "ADA002"

    def test_dates_too_far_apart_not_grouped(self):
        rows = [
            make_row(ada="ADA001", amount="100000", issue_date="2022-01-01"),
            make_row(ada="ADA002", amount="100000", issue_date="2024-06-01"),  # > 180 days
        ]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        assert len(contracts) == 2

    def test_lifecycle_map_covers_all_adas(self):
        rows = [
            make_row(ada="ADA001", amount="100000", issue_date="2024-06-01"),
            make_row(ada="ADA002", amount="100000", issue_date="2024-06-15"),
            make_row(ada="ADA003", amount="0"),
        ]
        contracts, lifecycle = lpl.link_lifecycle(rows)
        lifecycle_adas = {r["ada"] for r in lifecycle}
        assert {"ADA001", "ADA002", "ADA003"} == lifecycle_adas

    def test_deduplication_reduces_spend(self):
        # Three stages of same €9.5M contract
        rows = [
            make_row(ada="ADA001", decision_type="ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ", amount="9500000", issue_date="2019-11-01"),
            make_row(ada="ADA002", decision_type="ΚΑΤΑΚΥΡΩΣΗ", amount="9500000", issue_date="2019-12-03"),
            make_row(ada="ADA003", decision_type="ΣΥΜΒΑΣΗ", amount="9500000", issue_date="2020-01-15"),
        ]
        contracts, _ = lpl.link_lifecycle(rows)
        assert len(contracts) == 1
        total = sum(lpl.safe_float(c["amount"]) for c in contracts)
        assert total == 9_500_000  # not 3× that


class TestParseDate:
    def test_iso_format(self):
        from datetime import datetime
        d = lpl.parse_date("2024-06-15")
        assert d == datetime(2024, 6, 15)

    def test_greek_format(self):
        from datetime import datetime
        d = lpl.parse_date("15/06/2024")
        assert d == datetime(2024, 6, 15)

    def test_unix_ms_timestamp(self):
        # 1549929600000 ms = 2019-02-12
        d = lpl.parse_date("1549929600000")
        assert d is not None
        assert d.year == 2019

    def test_none_returns_none(self):
        assert lpl.parse_date(None) is None
        assert lpl.parse_date("") is None
