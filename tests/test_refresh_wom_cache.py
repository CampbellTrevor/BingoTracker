import io
import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import refresh_wom_cache


def json_response(payload):
    return io.BytesIO(json.dumps(payload).encode("utf-8"))


class RefreshWomCacheTests(unittest.TestCase):
    def assert_inclusive_event_query(self, mocked_urlopen):
        request = mocked_urlopen.call_args.args[0]
        query = parse_qs(urlparse(request.full_url).query)
        self.assertEqual(query["startDate"], ["2026-09-11"])
        self.assertEqual(query["endDate"], ["2026-09-19"])

    def test_bulk_gains_includes_the_logical_event_end_date(self):
        with patch.object(
            refresh_wom_cache,
            "urlopen",
            return_value=json_response([]),
        ) as mocked_urlopen:
            self.assertEqual(
                refresh_wom_cache._fetch_bulk_gains(
                    11794,
                    "2026-09-11",
                    "2026-09-18",
                ),
                [],
            )

        self.assert_inclusive_event_query(mocked_urlopen)

    def test_player_gains_includes_the_logical_event_end_date(self):
        with patch.object(
            refresh_wom_cache,
            "urlopen",
            return_value=json_response({"data": {}}),
        ) as mocked_urlopen:
            self.assertEqual(
                refresh_wom_cache._fetch_player_gains(
                    "Test Player",
                    "2026-09-11",
                    "2026-09-18",
                ),
                {"data": {}},
            )

        self.assert_inclusive_event_query(mocked_urlopen)

    def test_cache_metadata_keeps_the_logical_event_end_date(self):
        payload = refresh_wom_cache.build_cache_payload(
            rows=[
                {
                    "player": {"username": "Test Player"},
                    "data": [
                        {
                            "metric": "chambers_of_xeric",
                            "gained": 6,
                        }
                    ],
                }
            ],
            group_id=11794,
            start_date="2026-09-11",
            end_date="2026-09-18",
            requested_metrics={"chambers_of_xeric"},
        )

        self.assertEqual(payload["start_date"], "2026-09-11")
        self.assertEqual(payload["end_date"], "2026-09-18")
        self.assertEqual(
            payload["metrics"]["chambers_of_xeric"]["testplayer"],
            6,
        )


if __name__ == "__main__":
    unittest.main()
