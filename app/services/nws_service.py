#!/usr/bin/env python3
"""
National Weather Service API Integration

Fetches active alerts and forecasts from the NWS API for weather report enhancement.
Documentation: https://www.weather.gov/documentation/services-web-api
"""
import logging
import requests
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger("nws-service")

# NWS API base URL
NWS_API_BASE = "https://api.weather.gov"

# Location configuration - Arlington, VA area
NWS_LATITUDE = 38.7692672
NWS_LONGITUDE = -77.0890822

# User agent required by NWS API
NWS_USER_AGENT = "(weather-llm-service, dbm82@dreampipestudios.com)"


def get_nws_alerts(latitude: float = NWS_LATITUDE, longitude: float = NWS_LONGITUDE) -> List[Dict]:
    """
    Fetch active weather alerts for the specified location from NWS API.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        
    Returns:
        List of active alert dictionaries with relevant information
    """
    try:
        # Get alerts for the point
        url = f"{NWS_API_BASE}/alerts/active"
        params = {
            "point": f"{latitude},{longitude}"
        }
        headers = {
            "User-Agent": NWS_USER_AGENT,
            "Accept": "application/geo+json"
        }
        
        logger.info(f"Fetching NWS alerts for location: {latitude},{longitude}")
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"NWS alerts API returned status {response.status_code}: {response.text}")
            return []
        
        data = response.json()
        features = data.get("features", [])
        
        if not features:
            logger.info("No active alerts for this location")
            return []
        
        # Extract relevant alert information
        alerts = []
        for feature in features:
            props = feature.get("properties", {})
            alert_info = {
                "event": props.get("event"),  # e.g., "Winter Storm Warning"
                "severity": props.get("severity"),  # "Extreme", "Severe", "Moderate", "Minor"
                "urgency": props.get("urgency"),  # "Immediate", "Expected", "Future"
                "certainty": props.get("certainty"),  # "Observed", "Likely", "Possible"
                "headline": props.get("headline"),
                "description": props.get("description"),
                "instruction": props.get("instruction"),
                "onset": props.get("onset"),  # Start time
                "expires": props.get("expires"),  # Expiration time
                "areaDesc": props.get("areaDesc"),  # Geographic area
            }
            alerts.append(alert_info)
            logger.info(f"Found alert: {alert_info['event']} (Severity: {alert_info['severity']})")
        
        return alerts
        
    except requests.exceptions.Timeout:
        logger.error("NWS alerts API request timed out")
        return []
    except Exception as e:
        logger.error(f"Error fetching NWS alerts: {str(e)}")
        return []


def get_nws_forecast(latitude: float = NWS_LATITUDE, longitude: float = NWS_LONGITUDE) -> Optional[Dict]:
    """
    Fetch weather forecast for the specified location from NWS API.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        
    Returns:
        Dictionary with forecast information or None if failed
    """
    try:
        # Step 1: Get the gridpoint for this location
        url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
        headers = {
            "User-Agent": NWS_USER_AGENT,
            "Accept": "application/geo+json"
        }
        
        logger.info(f"Fetching NWS gridpoint for location: {latitude},{longitude}")
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"NWS points API returned status {response.status_code}: {response.text}")
            return None
        
        point_data = response.json()
        props = point_data.get("properties", {})
        
        # Extract forecast URL and astronomical metadata
        forecast_url = props.get("forecast")
        forecast_hourly_url = props.get("forecastHourly")
        astronomical_data = props.get("astronomicalData") or {}
        
        if not forecast_url:
            logger.error("No forecast URL found in NWS points response")
            return None
        
        # Step 2: Get the forecast
        logger.info(f"Fetching forecast from: {forecast_url}")
        forecast_response = requests.get(forecast_url, headers=headers, timeout=10)
        
        if forecast_response.status_code != 200:
            logger.error(f"NWS forecast API returned status {forecast_response.status_code}")
            return None
        
        forecast_data = forecast_response.json()
        periods = forecast_data.get("properties", {}).get("periods", [])
        
        if not periods:
            logger.warning("No forecast periods found")
            return None
        
        # Extract relevant forecast information (next 24 hours)
        forecast_summary = {
            "office": props.get("forecastOffice"),
            "gridId": props.get("gridId"),
            "gridX": props.get("gridX"),
            "gridY": props.get("gridY"),
            "sunrise": astronomical_data.get("sunrise"),
            "sunset": astronomical_data.get("sunset"),
            "periods": []
        }
        
        # Get first few periods (typically covers today and tonight)
        for period in periods[:4]:  # Get next ~2 days of periods
            period_info = {
                "name": period.get("name"),  # "This Afternoon", "Tonight", etc.
                "startTime": period.get("startTime"),
                "endTime": period.get("endTime"),
                "temperature": period.get("temperature"),
                "temperatureUnit": period.get("temperatureUnit"),
                "windSpeed": period.get("windSpeed"),
                "windDirection": period.get("windDirection"),
                "shortForecast": period.get("shortForecast"),
                "detailedForecast": period.get("detailedForecast"),
            }
            forecast_summary["periods"].append(period_info)
            logger.info(f"Forecast period: {period_info['name']} - {period_info['shortForecast']}")
        
        return forecast_summary
        
    except requests.exceptions.Timeout:
        logger.error("NWS forecast API request timed out")
        return None
    except Exception as e:
        logger.error(f"Error fetching NWS forecast: {str(e)}")
        return None


def get_nws_data(latitude: float = NWS_LATITUDE, longitude: float = NWS_LONGITUDE) -> Dict:
    """
    Fetch both alerts and forecast data from NWS API.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        
    Returns:
        Dictionary containing alerts and forecast data
    """
    return {
        "alerts": get_nws_alerts(latitude, longitude),
        "forecast": get_nws_forecast(latitude, longitude),
        "location": {
            "latitude": latitude,
            "longitude": longitude
        },
        "fetched_at": datetime.utcnow().isoformat()
    }


def format_alerts_for_prompt(alerts: List[Dict]) -> str:
    """
    Format NWS alerts into a readable string for LLM prompt.
    
    Args:
        alerts: List of alert dictionaries
        
    Returns:
        Formatted string describing active alerts
    """
    if not alerts:
        return "No active weather alerts."
    
    alert_text = f"ACTIVE WEATHER ALERTS ({len(alerts)} total):\n"
    
    for i, alert in enumerate(alerts, 1):
        alert_text += f"\n{i}. {alert['event']} (Severity: {alert['severity']}, Urgency: {alert['urgency']})\n"
        alert_text += f"   Area: {alert['areaDesc']}\n"
        
        if alert['headline']:
            alert_text += f"   Headline: {alert['headline']}\n"
        
        # Include onset and expiration times
        if alert['onset']:
            alert_text += f"   Begins: {alert['onset']}\n"
        if alert['expires']:
            alert_text += f"   Expires: {alert['expires']}\n"
        
        # Add description (truncate if too long)
        if alert['description']:
            desc = alert['description']
            if len(desc) > 500:
                desc = desc[:500] + "..."
            alert_text += f"   Description: {desc}\n"
        
        # Add instructions if available
        if alert['instruction']:
            inst = alert['instruction']
            if len(inst) > 300:
                inst = inst[:300] + "..."
            alert_text += f"   Instructions: {inst}\n"
    
    return alert_text


def format_forecast_for_prompt(forecast: Optional[Dict], include_solar_timing: bool = True) -> str:
    """
    Format NWS forecast into a readable string for LLM prompt.
    
    Args:
        forecast: Forecast dictionary
        
    Returns:
        Formatted string describing the forecast
    """
    if not forecast or not forecast.get("periods"):
        return "No forecast data available."
    
    forecast_text = "NATIONAL WEATHER SERVICE FORECAST:\n"

    if include_solar_timing:
        sunrise = forecast.get("sunrise")
        sunset = forecast.get("sunset")
        if sunrise or sunset:
            forecast_text += "\nSolar timing:\n"
            if sunrise:
                forecast_text += f"  Sunrise: {sunrise}\n"
            if sunset:
                forecast_text += f"  Sunset: {sunset}\n"
    
    for period in forecast["periods"]:
        forecast_text += f"\n{period['name']}:\n"
        forecast_text += f"  Temperature: {period['temperature']}°{period['temperatureUnit']}\n"
        forecast_text += f"  Wind: {period['windSpeed']} {period['windDirection']}\n"
        forecast_text += f"  Conditions: {period['shortForecast']}\n"
        
        if period.get('detailedForecast'):
            forecast_text += f"  Details: {period['detailedForecast']}\n"
    
    return forecast_text


def format_daylight_for_prompt(forecast: Optional[Dict], twilight_buffer_minutes: int = 45) -> str:
    """
    Format sunrise/sunset context into a prompt-friendly summary.

    Args:
        forecast: Forecast dictionary returned by get_nws_forecast
        twilight_buffer_minutes: Minutes to apply before sunrise and after sunset

    Returns:
        Formatted daylight context string, or empty string when unavailable
    """
    if not forecast:
        return ""

    sunrise = forecast.get("sunrise")
    sunset = forecast.get("sunset")
    if not sunrise and not sunset:
        return ""

    lines = ["DAYLIGHT CONTEXT (NWS):"]
    if sunrise:
        lines.append(f"- Sunrise: {sunrise}")
    if sunset:
        lines.append(f"- Sunset: {sunset}")
    lines.append(f"- Twilight buffer: {twilight_buffer_minutes} minutes before sunrise and after sunset")
    return "\n".join(lines)


def compare_forecast_to_observations(forecast: Optional[Dict], weather_summary: Dict) -> str:
    """
    Compare NWS forecast to actual observations for LLM analysis.
    
    Args:
        forecast: NWS forecast data
        weather_summary: Observed weather summary from measurements
        
    Returns:
        Formatted comparison text
    """
    if not forecast or not forecast.get("periods"):
        return ""
    
    comparison = "\n\nFORECAST vs OBSERVATIONS COMPARISON:\n"
    
    # Get current/recent period from forecast
    current_period = forecast["periods"][0] if forecast["periods"] else None
    
    if current_period:
        forecast_temp = current_period.get("temperature")
        forecast_temp_unit = current_period.get("temperatureUnit", "F")
        
        # Convert our observed temps (in Celsius) to Fahrenheit for comparison
        observed_temp_avg = weather_summary.get("temperature", {}).get("avg", 0)
        observed_temp_f = (observed_temp_avg * 9/5) + 32
        
        comparison += f"Forecast predicts {forecast_temp}°{forecast_temp_unit} for {current_period['name']}\n"
        comparison += f"Your sensors observed average: {observed_temp_avg:.1f}°C ({observed_temp_f:.1f}°F)\n"
        
        if forecast_temp:
            temp_diff = abs(forecast_temp - observed_temp_f)
            if temp_diff > 5:
                comparison += f"Note: {temp_diff:.1f}°F difference between forecast and observations\n"
    
    return comparison
