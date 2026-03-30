import unittest
from unittest.mock import patch

from app.services.llm_service import _truncate_forecast_for_prompt
from app.services.nws_service import get_nws_forecast


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


class NwsForecastSplitPathTests(unittest.TestCase):
    @patch("app.services.nws_service.requests.get")
    def test_get_nws_forecast_keeps_all_periods(self, mock_get):
        periods = []
        for i in range(6):
            periods.append(
                {
                    "name": f"Period {i + 1}",
                    "startTime": "2026-03-30T00:00:00+00:00",
                    "endTime": "2026-03-30T01:00:00+00:00",
                    "temperature": 70 + i,
                    "temperatureUnit": "F",
                    "windSpeed": "10 mph",
                    "windDirection": "NW",
                    "shortForecast": "Partly Cloudy",
                    "detailedForecast": "Sample forecast text",
                }
            )

        points_payload = {
            "properties": {
                "forecast": "https://api.weather.gov/gridpoints/LWX/1,1/forecast",
                "forecastOffice": "LWX",
                "gridId": "LWX",
                "gridX": 1,
                "gridY": 1,
                "astronomicalData": {
                    "sunrise": "2026-03-30T06:55:00-04:00",
                    "sunset": "2026-03-30T19:26:00-04:00",
                },
            }
        }
        forecast_payload = {
            "properties": {
                "generatedAt": "2026-03-30T12:00:00+00:00",
                "updateTime": "2026-03-30T11:58:00+00:00",
                "validTimes": "2026-03-30T12:00:00+00:00/P7D",
                "periods": periods,
            }
        }

        mock_get.side_effect = [
            _FakeResponse(200, points_payload),
            _FakeResponse(200, forecast_payload),
        ]

        forecast = get_nws_forecast()
        self.assertIsNotNone(forecast)
        self.assertEqual(len(forecast["periods"]), 6)

    def test_truncate_forecast_for_prompt_uses_first_four_periods(self):
        forecast = {
            "office": "LWX",
            "periods": [{"name": f"P{i}"} for i in range(1, 8)],
        }

        truncated = _truncate_forecast_for_prompt(forecast, max_periods=4)

        self.assertEqual(len(truncated["periods"]), 4)
        self.assertEqual(truncated["periods"][0]["name"], "P1")
        self.assertEqual(truncated["periods"][3]["name"], "P4")

    def test_truncate_forecast_preserves_other_fields(self):
        forecast = {
            "office": "LWX",
            "generatedAt": "2026-03-30T12:00:00+00:00",
            "periods": [{"name": "A"}, {"name": "B"}],
        }

        truncated = _truncate_forecast_for_prompt(forecast, max_periods=1)

        self.assertEqual(truncated["office"], "LWX")
        self.assertEqual(truncated["generatedAt"], "2026-03-30T12:00:00+00:00")
        self.assertEqual(len(truncated["periods"]), 1)


if __name__ == "__main__":
    unittest.main()
