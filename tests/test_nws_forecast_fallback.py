import unittest
from datetime import datetime, timezone

from app.services.llm_service import _ensure_forecast_with_recent_fallback


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    def __iter__(self):
        return iter(self.docs)


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, query=None, projection=None, sort=None, limit=0):
        items = list(self.docs)

        if sort:
            field, direction = sort[0]
            reverse = direction == -1
            items.sort(key=lambda d: d.get(field), reverse=reverse)

        if limit:
            items = items[:limit]

        return FakeCursor(items)


class FakeDB:
    def __init__(self, snapshots):
        self.collections = {
            "nws_snapshots": FakeCollection(snapshots),
        }

    def __getitem__(self, name):
        return self.collections[name]


class NwsForecastFallbackTests(unittest.TestCase):
    def test_uses_most_recent_non_null_from_previous_three(self):
        snapshots = [
            {
                "created_at": datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 45, tzinfo=timezone.utc),
                "nws_data": {"forecast": {"periods": [{"name": "Morning"}] }},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
                "nws_data": {"forecast": {"periods": [{"name": "Earlier"}] }},
            },
        ]
        db = FakeDB(snapshots)

        nws_data = {"alerts": [], "forecast": None}
        resolved = _ensure_forecast_with_recent_fallback(nws_data, db=db, max_previous=3)

        self.assertIsNotNone(resolved["forecast"])
        self.assertEqual(resolved["forecast"]["periods"][0]["name"], "Morning")

    def test_hard_fails_when_previous_three_forecasts_are_all_null(self):
        snapshots = [
            {
                "created_at": datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 45, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
        ]
        db = FakeDB(snapshots)

        with self.assertRaises(RuntimeError):
            _ensure_forecast_with_recent_fallback({"forecast": None}, db=db, max_previous=3)

    def test_ignores_older_non_null_forecast_outside_previous_three(self):
        snapshots = [
            {
                "created_at": datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 45, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc),
                "nws_data": {"forecast": None},
            },
            {
                "created_at": datetime(2026, 3, 30, 9, 15, tzinfo=timezone.utc),
                "nws_data": {"forecast": {"periods": [{"name": "Too Old"}] }},
            },
        ]
        db = FakeDB(snapshots)

        with self.assertRaises(RuntimeError):
            _ensure_forecast_with_recent_fallback({"forecast": None}, db=db, max_previous=3)


if __name__ == "__main__":
    unittest.main()
