import unittest
import asyncio
from datetime import datetime, timezone

from app.api import routes


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, query=None, sort=None):
        query = query or {}
        filtered = []
        for doc in self.docs:
            matches = True
            for key, expected in query.items():
                if doc.get(key) != expected:
                    matches = False
                    break
            if matches:
                filtered.append(doc)

        if not filtered:
            return None

        if sort:
            field, direction = sort[0]
            reverse = direction == -1
            filtered.sort(key=lambda d: d.get(field), reverse=reverse)

        return filtered[0]


class FakeDB:
    def __init__(self, snapshots):
        self.collections = {
            "nws_snapshots": FakeCollection(snapshots),
        }

    def __getitem__(self, name):
        return self.collections[name]


class NwsCacheApiTests(unittest.TestCase):
    def setUp(self):
        self.snapshots = [
            {
                "_id": object(),
                "report_date": "2026-03-27",
                "location": "Arlington",
                "created_at": datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
                "fetched_at": "2026-03-27T11:58:00Z",
                "nws_data": {
                    "alerts": [{"event": "Flood Watch"}],
                    "forecast": {"periods": [{"name": "Tonight"}]},
                    "location": {"latitude": 38.7, "longitude": -77.0},
                },
            },
            {
                "_id": object(),
                "report_date": "2026-03-28",
                "location": "Arlington",
                "created_at": datetime(2026, 3, 28, 1, 0, tzinfo=timezone.utc),
                "fetched_at": "2026-03-28T00:59:00Z",
                "nws_data": {
                    "alerts": [],
                    "forecast": {"periods": [{"name": "Saturday"}]},
                    "location": {"latitude": 38.7, "longitude": -77.0},
                },
            },
            {
                "_id": object(),
                "report_date": "2026-03-28",
                "location": "Arlington",
                "created_at": datetime(2026, 3, 28, 2, 0, tzinfo=timezone.utc),
                "fetched_at": "2026-03-28T01:59:00Z",
                "nws_data": {
                    "alerts": [{"event": "Wind Advisory"}],
                    "forecast": {"periods": [{"name": "Saturday Night"}]},
                    "location": {"latitude": 38.7, "longitude": -77.0},
                },
            },
        ]
        self.db = FakeDB(self.snapshots)

    def test_get_latest_nws_snapshot(self):
        body = asyncio.run(routes.get_latest_nws_snapshot(db=self.db))
        self.assertTrue(body["success"])
        self.assertEqual(body["nws_snapshot"]["report_date"], "2026-03-28")
        self.assertEqual(body["nws_snapshot"]["nws_data"]["alerts"][0]["event"], "Wind Advisory")
        self.assertTrue(body["nws_snapshot"]["created_at"].endswith("Z"))

    def test_get_nws_snapshot_by_date_returns_latest_for_that_date(self):
        body = asyncio.run(routes.get_nws_snapshot_by_date("2026-03-28", db=self.db))
        self.assertTrue(body["success"])
        self.assertEqual(body["nws_snapshot"]["report_date"], "2026-03-28")
        self.assertEqual(body["nws_snapshot"]["nws_data"]["alerts"][0]["event"], "Wind Advisory")

    def test_get_nws_snapshot_by_date_not_found(self):
        body = asyncio.run(routes.get_nws_snapshot_by_date("2026-03-20", db=self.db))
        self.assertFalse(body["success"])
        self.assertIsNone(body["nws_snapshot"])


if __name__ == "__main__":
    unittest.main()
