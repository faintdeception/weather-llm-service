import unittest

from app.services.nws_service import format_forecast_for_prompt


class NwsPromptSolarExclusionTests(unittest.TestCase):
    def test_format_forecast_can_exclude_solar_timing(self):
        forecast = {
            "sunrise": "2026-03-28T06:57:25-04:00",
            "sunset": "2026-03-28T19:29:15-04:00",
            "periods": [
                {
                    "name": "This Afternoon",
                    "temperature": 67,
                    "temperatureUnit": "F",
                    "windSpeed": "10 mph",
                    "windDirection": "NW",
                    "shortForecast": "Sunny",
                    "detailedForecast": "Sunny, with a high near 67.",
                }
            ],
        }

        text_without_solar = format_forecast_for_prompt(forecast, include_solar_timing=False)
        self.assertNotIn("Solar timing", text_without_solar)
        self.assertNotIn("Sunrise:", text_without_solar)
        self.assertNotIn("Sunset:", text_without_solar)

        text_with_solar = format_forecast_for_prompt(forecast, include_solar_timing=True)
        self.assertIn("Solar timing", text_with_solar)
        self.assertIn("Sunrise:", text_with_solar)
        self.assertIn("Sunset:", text_with_solar)


if __name__ == "__main__":
    unittest.main()
