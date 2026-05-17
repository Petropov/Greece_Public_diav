"""Tests for scripts/fetch_windowed.py — date-windowed cap-busting re-fetcher."""
import json
import sys
from calendar import monthrange
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importlib
fetch_windowed = importlib.import_module("scripts.fetch_windowed")


class TestMonthWindows:
    def test_february_non_leap(self):
        windows = fetch_windowed.month_windows(2023, 2, window_days=7)
        # Feb 2023: 28 days → 4 windows
        assert len(windows) == 4
        assert windows[0] == (date(2023, 2, 1), date(2023, 2, 7))
        assert windows[-1][1] == date(2023, 2, 28)

    def test_february_leap(self):
        windows = fetch_windowed.month_windows(2024, 2, window_days=7)
        # Feb 2024: 29 days → 5 windows (last is 1 day)
        assert windows[-1][1] == date(2024, 2, 29)
        # All windows are contiguous and cover the full month
        days_covered = sum((w[1] - w[0]).days + 1 for w in windows)
        assert days_covered == 29

    def test_31_day_month(self):
        windows = fetch_windowed.month_windows(2024, 1, window_days=7)
        # Jan: 31 days → 5 windows (4×7 + 1×3)
        assert len(windows) == 5
        assert windows[-1][1] == date(2024, 1, 31)

    def test_daily_windows(self):
        windows = fetch_windowed.month_windows(2024, 6, window_days=1)
        assert len(windows) == 30
        for w in windows:
            assert w[0] == w[1]

    def test_windows_are_contiguous(self):
        for month in [1, 3, 6, 8, 11]:
            windows = fetch_windowed.month_windows(2023, month, window_days=7)
            for i in range(len(windows) - 1):
                gap = (windows[i + 1][0] - windows[i][1]).days
                assert gap == 1, f"Gap between windows {i} and {i+1} is {gap}"

    def test_large_window_is_whole_month(self):
        windows = fetch_windowed.month_windows(2023, 6, window_days=31)
        assert len(windows) == 1
        assert windows[0] == (date(2023, 6, 1), date(2023, 6, 30))


class TestUniqueAdaCount:
    def test_counts_unique_adas(self, tmp_path):
        rows = [{"ada": "A"}, {"ada": "B"}, {"ada": "A"}, {"ada": "C"}]
        export = tmp_path / "search_export.json"
        export.write_text(json.dumps({"decisionResultList": rows}))
        assert fetch_windowed.unique_ada_count(export) == 3

    def test_missing_file_returns_zero(self, tmp_path):
        assert fetch_windowed.unique_ada_count(tmp_path / "nonexistent.json") == 0

    def test_empty_list_returns_zero(self, tmp_path):
        export = tmp_path / "search_export.json"
        export.write_text(json.dumps({"decisionResultList": []}))
        assert fetch_windowed.unique_ada_count(export) == 0

    def test_rows_without_ada_ignored(self, tmp_path):
        rows = [{"ada": "A"}, {"subject": "no ada here"}, {"ada": ""}]
        export = tmp_path / "search_export.json"
        export.write_text(json.dumps({"decisionResultList": rows}))
        assert fetch_windowed.unique_ada_count(export) == 1


class TestRefetchMonth:
    def _make_cache(self, tmp_path, year, month, ada_count=500):
        """Create a fake capped month cache."""
        month_dir = tmp_path / f"organization_uid=6166" / f"year={year}" / f"month={month:02d}"
        month_dir.mkdir(parents=True)
        rows = [{"ada": f"ADA{i:04d}", "subject": f"Decision {i}"} for i in range(ada_count)]
        (month_dir / "search_export.json").write_text(
            json.dumps({"decisionResultList": rows})
        )
        return month_dir

    def test_dry_run_does_not_fetch(self, tmp_path):
        self._make_cache(tmp_path, 2024, 6, ada_count=500)
        with patch.object(fetch_windowed, "fetch_window") as mock_fetch:
            result = fetch_windowed.refetch_month(
                "6166", 2024, 6, tmp_path, dry_run=True
            )
        mock_fetch.assert_not_called()
        assert result["status"] == "dry_run"

    def test_skips_uncapped_month(self, tmp_path):
        self._make_cache(tmp_path, 2024, 1, ada_count=380)
        with patch.object(fetch_windowed, "fetch_window") as mock_fetch:
            result = fetch_windowed.refetch_month("6166", 2024, 1, tmp_path)
        mock_fetch.assert_not_called()
        assert result["status"] == "skipped_not_capped"

    def test_merges_and_deduplicates(self, tmp_path):
        self._make_cache(tmp_path, 2024, 6, ada_count=500)

        # Two windows returning overlapping ADAs
        window_responses = [
            [{"ada": f"W1_{i}"} for i in range(300)],
            [{"ada": f"W1_{i}"} for i in range(100)] + [{"ada": f"W2_{i}"} for i in range(200)],
        ]
        call_count = [0]

        def fake_fetch(org, start, end, **kwargs):
            resp = window_responses[call_count[0] % len(window_responses)]
            call_count[0] += 1
            return resp

        with patch.object(fetch_windowed, "fetch_window", side_effect=fake_fetch):
            result = fetch_windowed.refetch_month(
                "6166", 2024, 6, tmp_path, window_days=15
            )

        assert result["status"] == "refetched"
        # 500 existing cache (ADA0000-ADA0499) + 300 from window1 + 200 new from window2 = 1000
        # (W1 and W2 rows don't overlap with the cache, so all 500 new window rows are added)
        assert result["after"] == 1000

    def test_writes_metadata(self, tmp_path):
        self._make_cache(tmp_path, 2024, 6, ada_count=500)
        with patch.object(fetch_windowed, "fetch_window", return_value=[{"ada": "X1"}]):
            fetch_windowed.refetch_month("6166", 2024, 6, tmp_path, window_days=31)

        meta_path = tmp_path / "organization_uid=6166" / "year=2024" / "month=06" / "fetch_metadata.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert "windowed_refetch" in meta
        assert meta["windowed_refetch"]["unique_adas_before"] == 500


class TestParseMonthRange:
    def test_single_month(self):
        result = fetch_windowed.parse_month_range("2024-06")
        assert result == [(2024, 6)]

    def test_range(self):
        result = fetch_windowed.parse_month_range("2024-11:2025-02")
        assert result == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]

    def test_same_month_range(self):
        result = fetch_windowed.parse_month_range("2024-06:2024-06")
        assert result == [(2024, 6)]

    def test_cross_year(self):
        result = fetch_windowed.parse_month_range("2023-10:2024-03")
        assert len(result) == 6
        assert result[0] == (2023, 10)
        assert result[-1] == (2024, 3)
