import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from app.services import llm_service


class SolarEventPromptContextTests(unittest.TestCase):
    def test_does_not_include_solar_events_when_far_from_window(self):
        tz = ZoneInfo("America/New_York")
        now_local = datetime(2026, 3, 28, 17, 15, tzinfo=tz)
        forecast = {
            "sunrise": "2026-03-28T06:57:25-04:00",
            "sunset": "2026-03-28T19:29:15-04:00",
            "periods": [],
        }

        context = llm_service._build_solar_event_prompt_context(forecast, now_local)

        self.assertFalse(context["include_solar_timing"])
        self.assertEqual(context["event_lines"], [])

    def test_includes_sunset_when_within_window(self):
        tz = ZoneInfo("America/New_York")
        now_local = datetime(2026, 3, 28, 18, 45, tzinfo=tz)
        forecast = {
            "sunrise": "2026-03-28T06:57:25-04:00",
            "sunset": "2026-03-28T19:29:15-04:00",
            "periods": [],
        }

        context = llm_service._build_solar_event_prompt_context(forecast, now_local)

        self.assertTrue(context["include_solar_timing"])
        self.assertTrue(any("sunset" in line.lower() for line in context["event_lines"]))


if __name__ == "__main__":
    unittest.main()
