import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import digest_monthly


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class DigestMonthlyCacheTests(unittest.TestCase):
    def test_cache_hit_avoids_api_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = digest_monthly.search_cache_path(tmp, "6166", 2026, 4)
            digest_monthly.write_json(
                cache_path,
                {"decisionResultList": [{"ada": "CACHED", "issueDate": "01/04/2026 00:00:00"}]},
            )

            with patch("digest_monthly.requests.get") as get:
                df = digest_monthly.fetch_month_export(tmp, "6166", 2026, 4)

            get.assert_not_called()
            self.assertEqual(df.iloc[0]["ada"], "CACHED")

    def test_cache_miss_writes_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {"decisionResultList": [{"ada": "MISS"}]}
            with patch("digest_monthly.requests.get", return_value=FakeResponse(payload)) as get:
                df = digest_monthly.fetch_month_export(tmp, "6166", 2026, 4)

            get.assert_called_once()
            self.assertEqual(df.iloc[0]["ada"], "MISS")
            written = digest_monthly.search_cache_path(tmp, "6166", 2026, 4)
            self.assertTrue(written.exists())
            self.assertEqual(digest_monthly.read_json(written), payload)

    def test_force_refresh_refetches_and_overwrites_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = digest_monthly.search_cache_path(tmp, "6166", 2026, 4)
            digest_monthly.write_json(cache_path, {"decisionResultList": [{"ada": "STALE"}]})
            payload = {"decisionResultList": [{"ada": "FRESH"}]}

            with patch("digest_monthly.requests.get", return_value=FakeResponse(payload)) as get:
                df = digest_monthly.fetch_month_export(
                    tmp, "6166", 2026, 4, force_refresh=True
                )

            get.assert_called_once()
            self.assertEqual(df.iloc[0]["ada"], "FRESH")
            self.assertEqual(digest_monthly.read_json(cache_path), payload)

    def test_decision_detail_cache_uses_ada_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {"ada": "ADA-1", "subject": "Cached detail"}
            path = digest_monthly.decision_cache_path(tmp, "6166", 2026, 4, "ADA-1")
            digest_monthly.write_json(path, payload)

            with patch("digest_monthly.requests.get") as get:
                detail = digest_monthly.fetch_cached_decision_detail(tmp, "6166", 2026, 4, "ADA-1")

            get.assert_not_called()
            self.assertEqual(detail, payload)
            self.assertEqual(Path(path).name, "ADA-1.json")


if __name__ == "__main__":
    unittest.main()
