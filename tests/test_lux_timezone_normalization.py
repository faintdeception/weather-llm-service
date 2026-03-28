import unittest
from datetime import datetime, timezone

from app.services.llm_service import analyze_lux_anomaly


class LuxTimezoneNormalizationTests(unittest.TestCase):
    def test_naive_utc_timestamp_is_treated_as_utc_not_local(self):
        nws_data = {
            "forecast": {
                "sunrise": "2026-03-28T06:57:25-04:00",
                "sunset": "2026-03-28T19:29:15-04:00",
                "periods": [],
            }
        }

        # 21:00 UTC == 17:00 America/New_York (before sunset).
        timestamp_aware_utc = datetime(2026, 3, 28, 21, 0, 0, tzinfo=timezone.utc)
        timestamp_naive_utc = datetime(2026, 3, 28, 21, 0, 0)

        aware_measurement = [{
            "timestamp_ms": timestamp_aware_utc,
            "fields": {"lux": 2500.0},
            "tags": {"location": "test"},
        }]
        naive_measurement = [{
            "timestamp_ms": timestamp_naive_utc,
            "fields": {"lux": 2500.0},
            "tags": {"location": "test"},
        }]

        aware_result = analyze_lux_anomaly(aware_measurement, nws_data=nws_data)
        naive_result = analyze_lux_anomaly(naive_measurement, nws_data=nws_data)

        self.assertEqual(aware_result["time_period"], "daylight")
        self.assertEqual(naive_result["time_period"], "daylight")
        self.assertEqual(aware_result["classification_source"], "nws_solar")
        self.assertEqual(naive_result["classification_source"], "nws_solar")


if __name__ == "__main__":
    unittest.main()
