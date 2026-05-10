import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import digest_monthly


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None, text=""):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise digest_monthly.requests.HTTPError(f"HTTP {self.status_code}")

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

    def test_429_does_not_overwrite_existing_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = digest_monthly.search_cache_path(tmp, "6166", 2026, 4)
            cached = {"decisionResultList": [{"ada": "CACHED"}]}
            digest_monthly.write_json(cache_path, cached)

            with patch(
                "digest_monthly.requests.get",
                return_value=FakeResponse({"error": "too many requests"}, status_code=429),
            ), patch("digest_monthly.time.sleep"):
                df = digest_monthly.fetch_month_export(
                    tmp, "6166", 2026, 4, force_refresh=True, max_retries=1, retry_sleep_seconds=0
                )

            self.assertEqual(df.iloc[0]["ada"], "CACHED")
            self.assertEqual(digest_monthly.read_json(cache_path), cached)
            metadata = digest_monthly.read_json(digest_monthly.metadata_path(tmp, "6166", 2026, 4))
            self.assertEqual(metadata["fetch_status"], "rate_limited")
            self.assertTrue(metadata["api_rate_limited"])
            self.assertTrue(digest_monthly.incomplete_marker_path(tmp, "6166", 2026, 4).exists())

    def test_retry_after_retry_succeeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {"decisionResultList": [{"ada": "AFTER"}]}
            responses = [
                FakeResponse({"error": "too many requests"}, status_code=429, headers={"Retry-After": "7"}),
                FakeResponse(payload),
            ]

            with patch("digest_monthly.requests.get", side_effect=responses) as get, patch(
                "digest_monthly.time.sleep"
            ) as sleep:
                df = digest_monthly.fetch_month_export(
                    tmp, "6166", 2026, 4, max_retries=1, retry_sleep_seconds=1
                )

            self.assertEqual(df.iloc[0]["ada"], "AFTER")
            self.assertEqual(get.call_count, 2)
            sleep.assert_called_once_with(7.0)
            metadata = digest_monthly.read_json(digest_monthly.metadata_path(tmp, "6166", 2026, 4))
            self.assertEqual(metadata["api_calls_attempted"], 2)
            self.assertEqual(metadata["http_status_codes"], [429, 200])

    def test_metadata_is_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {"decisionResultList": [{"ada": "META"}]}
            with patch("digest_monthly.requests.get", return_value=FakeResponse(payload)):
                digest_monthly.fetch_month_export(tmp, "6166", 2026, 4)

            metadata = digest_monthly.read_json(digest_monthly.metadata_path(tmp, "6166", 2026, 4))
            self.assertEqual(metadata["org"], "6166")
            self.assertEqual(metadata["year"], 2026)
            self.assertEqual(metadata["month"], 4)
            self.assertFalse(metadata["cache_hit"])
            self.assertFalse(metadata["force_refresh"])
            self.assertEqual(metadata["api_calls_attempted"], 1)
            self.assertFalse(metadata["api_rate_limited"])
            self.assertEqual(metadata["http_status_codes"], [200])
            self.assertEqual(metadata["fetch_status"], "success")
            self.assertIn("fetched_at", metadata)

    def test_incomplete_marker_is_written_on_repeated_rate_limit_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                "digest_monthly.requests.get",
                return_value=FakeResponse({"error": "threshold exceeded"}, status_code=429),
            ), patch("digest_monthly.time.sleep"):
                df = digest_monthly.fetch_month_export(
                    tmp, "6166", 2026, 4, max_retries=2, retry_sleep_seconds=0
                )

            self.assertTrue(df.empty)
            marker = digest_monthly.incomplete_marker_path(tmp, "6166", 2026, 4)
            self.assertTrue(marker.exists())
            self.assertIn("rate_limited", marker.read_text(encoding="utf-8"))

    def test_search_only_run_does_not_call_decision_detail_endpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {
                "decisionResultList": [
                    {
                        "ada": "SEARCH-ONLY",
                        "issueDate": "01/04/2026 00:00:00",
                        "submissionTimestamp": "01/04/2026 00:00:00",
                    }
                ]
            }
            args = SimpleNamespace(
                cache_dir=tmp,
                org="6166",
                force_refresh=False,
                max_retries=0,
                retry_sleep_seconds=0,
                search_only=True,
            )

            with patch("digest_monthly.requests.get", return_value=FakeResponse(payload)) as get, patch(
                "digest_monthly.fetch_decision_detail"
            ) as detail:
                digest_monthly.run_monthly_digest(args, 2026, 4)

            get.assert_called_once()
            detail.assert_not_called()

    def test_search_only_writes_monthly_search_cache_and_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {"decisionResultList": [{"ada": "SEARCH-CACHE"}]}
            args = SimpleNamespace(
                cache_dir=tmp,
                org="6166",
                force_refresh=False,
                max_retries=0,
                retry_sleep_seconds=0,
                search_only=True,
            )

            with patch("digest_monthly.requests.get", return_value=FakeResponse(payload)):
                digest_monthly.run_monthly_digest(args, 2026, 4)

            cache_path = digest_monthly.search_cache_path(tmp, "6166", 2026, 4)
            self.assertTrue(cache_path.exists())
            self.assertEqual(digest_monthly.read_json(cache_path), payload)
            metadata = digest_monthly.read_json(digest_monthly.metadata_path(tmp, "6166", 2026, 4))
            self.assertEqual(metadata["detail_enrichment"], "skipped")
            self.assertEqual(metadata["fetch_status"], "success")
            self.assertFalse(digest_monthly.incomplete_marker_path(tmp, "6166", 2026, 4).exists())

    def test_search_only_cache_hit_marks_metadata_without_incomplete_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = digest_monthly.search_cache_path(tmp, "6166", 2026, 4)
            digest_monthly.write_json(cache_path, {"decisionResultList": [{"ada": "CACHED-SEARCH"}]})
            args = SimpleNamespace(
                cache_dir=tmp,
                org="6166",
                force_refresh=False,
                max_retries=0,
                retry_sleep_seconds=0,
                search_only=True,
            )

            with patch("digest_monthly.requests.get") as get, patch("digest_monthly.fetch_decision_detail") as detail:
                digest_monthly.run_monthly_digest(args, 2026, 4)

            get.assert_not_called()
            detail.assert_not_called()
            metadata = digest_monthly.read_json(digest_monthly.metadata_path(tmp, "6166", 2026, 4))
            self.assertEqual(metadata["detail_enrichment"], "skipped")
            self.assertEqual(metadata["fetch_status"], "cache_hit")
            self.assertFalse(digest_monthly.incomplete_marker_path(tmp, "6166", 2026, 4).exists())

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
