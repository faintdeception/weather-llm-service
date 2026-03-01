#!/usr/bin/env python3
"""
LLM Service for Weather Reports

This module handles all interactions with the LLM API for generating
human-readable weather reports based on collected weather data.
Note: Output maintains prediction format for downstream compatibility.
"""
import os
import sys
import json
import logging
import traceback
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from pymongo import MongoClient
from app.database.connection import get_database, with_db_connection
from app.services.nws_service import (
    get_nws_data,
    format_alerts_for_prompt,
    format_forecast_for_prompt,
    compare_forecast_to_observations
)
from app.services.memory_service import (
    get_memory_context,
    get_recent_reasoning_openers,
    append_memory_entry,
    should_compact_memory,
    compact_memory_file,
)

def _configure_text_io():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:
            continue

_configure_text_io()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("llm-service")

# Configurable lookback window (hours) for measurement analysis
ANALYSIS_WINDOW_HOURS = float(os.getenv("ANALYSIS_WINDOW_HOURS", "3"))
LOCAL_TIMEZONE = os.getenv("LOCAL_TIMEZONE", "America/New_York")


def _get_local_time():
    """Return the current local time and timezone name, falling back to UTC if misconfigured."""
    try:
        tz = ZoneInfo(LOCAL_TIMEZONE)
        return datetime.now(tz), LOCAL_TIMEZONE
    except Exception as exc:
        logger.warning(f"Invalid LOCAL_TIMEZONE '{LOCAL_TIMEZONE}', defaulting to UTC: {exc}")
        return datetime.now(timezone.utc), "UTC"

@with_db_connection
def get_daily_report(date=None):
    """
    Retrieve a daily report from the database
    
    Args:
        date: Date string in YYYY-MM-DD format, defaults to yesterday
        
    Returns:
        The daily report document or None if not found    """
    try:
        db = get_database()
        
        if date is None:
            # Default to yesterday
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
            date = yesterday
            
        report = db['daily_reports'].find_one({'date': date})
        return report
    except Exception as e:
        logger.error(f"Error retrieving daily report: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

@with_db_connection
def get_trend_data(location):
    """
    Retrieve the latest trend data for a location
    
    Args:
        location: Location name
        
    Returns:
        The trend data document or None if not found
    """
    try:
        db = get_database()
        trend_data = db['trends'].find_one(
            {'location': location},
            sort=[('timestamp', -1)]
        )
        return trend_data
    except Exception as e:
        logger.error(f"Error retrieving trend data: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def call_prediction_api(weather_data):
    """
    Call an external LLM API to generate weather reports based on observed data
    
    Args:
        weather_data: Dictionary with weather summary data
        
    Returns:
        Dictionary with weather report results in prediction format for compatibility
    """
    try:
        # Get API key from environment variable
        api_key = os.environ.get('LLM_API_KEY')
        if not api_key:
            logger.error("LLM_API_KEY environment variable not set")
            return None
        
        # Get LLM API URL from environment or use default
        api_url = os.environ.get('LLM_API_URL', 'https://api.openai.com/v1/chat/completions')
        
        # Get LLM model from environment or use default
        model_name = os.environ.get('LLM_MODEL', 'gpt-4')

        # Get LLM temperature from environment or use default
        llm_temperature_raw = os.environ.get('LLM_TEMPERATURE', '0.8')
        try:
            llm_temperature = float(llm_temperature_raw)
        except ValueError:
            logger.warning(f"Invalid LLM_TEMPERATURE '{llm_temperature_raw}', defaulting to 0.8")
            llm_temperature = 0.8
        llm_temperature = min(max(llm_temperature, 0.0), 2.0)
        
        # Construct the prompt for the LLM
        current_local_time, tz_name = _get_local_time()
        memory_context = get_memory_context()
        recent_openers = get_recent_reasoning_openers(limit=8)
        prompt = f"""
    Based on the following weather data from {weather_data['location']} on {weather_data['date']}, please provide a weather analysis in the exact JSON format specified below.

    Current local time: {current_local_time.strftime('%Y-%m-%d %H:%M %Z')} (timezone: {tz_name}). Match your narrative and clothing suggestions to the time of day (morning/afternoon/evening/night) and avoid saying "day" if it is currently night.

    Don't use the slur "meatbags" or any derivatives in your response. Use terms like "humans", "squishies", "air breathers", "carbon units", or "earthlings" instead.
    
    Your name is WeatherBot. Provide the analysis in the style of Bender from Futurama, if he were a pre-adolescent kid, working as a robot weather reporter, but do not mention the name "Bender", "Benderbot" or any such derivatives explicitly in your response.

    You should use emjois, but sparingly, and only if they are cool ones like Bender would use.

    IMPORTANT STYLE VARIETY RULES:
    - Do not start every report with the same phrase.
    - Never start the reasoning with "Whoa, earthlings!".
    - Vary opening sentence structure and greeting language across runs.
    
    The report should be informative and based on the data, but digestable for humans/squishies/air breathers/carbon units/earthlings to read. Be sure to provide suggestions on what to wear!

    
"""

        if memory_context:
            prompt += f"""
Operational memory for continuity (most recent thread excerpt):
{memory_context}

Rules for using memory:
- Use memory for continuity of narrative tone and context only.
- Treat current measurements and NWS data as authoritative for weather facts.
- Do not copy stale values from memory when fresh data is provided.

"""

        if recent_openers:
            formatted_openers = ", ".join([f"'{opener}'" for opener in recent_openers])
            prompt += f"""
Recent opening phrases to avoid repeating:
{formatted_openers}

When writing reasoning, choose a fresh opening that is clearly different from the listed phrases.

"""

        prompt += f"""

Current weather summary:
Temperature: Min {weather_data['summary']['temperature']['min']:.2f}°C, Max {weather_data['summary']['temperature']['max']:.2f}°C, Avg {weather_data['summary']['temperature']['avg']:.2f}°C
Humidity: Min {weather_data['summary']['humidity']['min']:.2f}%, Max {weather_data['summary']['humidity']['max']:.2f}%, Avg {weather_data['summary']['humidity']['avg']:.2f}%
Pressure: Min {weather_data['summary']['pressure']['min']:.2f} hPa, Max {weather_data['summary']['pressure']['max']:.2f} hPa, Avg {weather_data['summary']['pressure']['avg']:.2f} hPa
Wind Speed: Min {weather_data['summary']['wind_speed']['min']:.2f} mph, Max {weather_data['summary']['wind_speed']['max']:.2f} mph, Avg {weather_data['summary']['wind_speed']['avg']:.2f} mph
"""
        
        # Add trend data if available
        if weather_data.get('recent_trends'):
            trend_window = weather_data.get('analysis_window_hours', ANALYSIS_WINDOW_HOURS)
            prompt += f"\nObserved trends (over last {trend_window} hours):"
            for param, trend_data in weather_data['recent_trends'].items():
                direction = trend_data.get('direction', 'stable')
                change = trend_data.get('change', 0)
                rate = trend_data.get('rate_per_hour', 0)
                
                prompt += f"\n{param.capitalize()}: {direction}, Change: {change:.2f}, Rate: {rate:.2f}/hour"

        # Add precipitation note if detected
        precipitation = weather_data.get('precipitation')
        if precipitation and precipitation.get('detected'):
            precip_window = weather_data.get('analysis_window_hours', ANALYSIS_WINDOW_HOURS)
            fields = ", ".join(precipitation.get('fields', []))
            detail = f" (fields: {fields})" if fields else ""
            prompt += f"\nPrecipitation detected in the last {precip_window} hours{detail}."
        
        # Add lux anomaly if detected (only when contextually interesting)
        lux_anomaly = weather_data.get('lux_anomaly')
        if lux_anomaly and lux_anomaly.get('anomalous'):
            prompt += f"\n\n⚠️ LIGHT LEVEL ANOMALY: {lux_anomaly['reason']}"
            # Extra emphasis if there are also weather alerts
            if weather_data.get('nws_data', {}).get('alerts'):
                prompt += " This correlates with active weather alerts!"
        
        # Add NWS alerts and forecast data
        nws_data = weather_data.get('nws_data')
        if nws_data:
            prompt += "\n\n" + "="*60 + "\n"
            prompt += format_alerts_for_prompt(nws_data.get('alerts', []))
            prompt += "\n" + "="*60 + "\n"
            prompt += format_forecast_for_prompt(nws_data.get('forecast'))
            prompt += compare_forecast_to_observations(nws_data.get('forecast'), weather_data['summary'])
            prompt += "\n" + "="*60
            
            # Add specific instruction about alerts
            if nws_data.get('alerts'):
                prompt += "\n\nIMPORTANT: Active weather alerts are present! Please comment on these exceptional weather conditions in your analysis and provide appropriate safety advice and clothing recommendations."
        
        prompt += """

CRITICAL: You must return the response in this EXACT JSON format:
{
  "prediction_12h": {
    "temperature": {
      "min": <number>,
      "max": <number>
    },
    "humidity": {
      "min": <number>,
      "max": <number>
    },
    "pressure": {
      "min": <number>,
      "max": <number>
    },
    "wind_speed": {
      "min": <number>,
      "max": <number>
    }
  },
  "prediction_24h": {
    "temperature": {
      "min": <number>,
      "max": <number>
    },
    "humidity": {
      "min": <number>,
      "max": <number>
    },
    "pressure": {
      "min": <number>,
      "max": <number>
    },
    "wind_speed": {
      "min": <number>,
      "max": <number>
    }
  },
  "reasoning": "<string describing your analysis of the observed weather patterns and trends and clothing suggestions>",
  "confidence": <number between 0.0 and 1.0>
}

For prediction_12h and prediction_24h, use the observed data ranges but you may extrapolate slightly based on trends. Focus on what the data shows rather than making wild predictions. The reasoning should explain the observed patterns and trends in the data."""
        
        # Log the API request (without the key)
        logger.info(f"Calling LLM API: {api_url}")
        logger.info(f"Using LLM model={model_name}, temperature={llm_temperature}")
        logger.debug(f"Prompt: {prompt}")
        
        # Call the LLM API
        response = requests.post(
            api_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": model_name,
                "temperature": llm_temperature,
                "messages": [
                    {"role": "system", "content": "You are WeatherBot, a fun and cool weather reporting robot that talks like Bender from Futurama, if he were a pre-adolescent kid, without ever saying the name 'Bender'. You analyze observed weather data and provide structured data in the exact format requested. Focus on observed data patterns and trends rather than future predictions, but format as if they were predictions for system compatibility. Use the provided local time to keep wording time-appropriate (e.g., say 'night' when it's late). Vary the opening sentence between runs and avoid repeating catchphrases."},
                    {"role": "user", "content": prompt}
                ],
                "response_format": {"type": "json_object"}
            },
            timeout=60  # Add timeout to prevent hanging requests
        )
        
        # Parse the response
        if response.status_code == 200:
            response_data = response.json()
            prediction_text = response_data['choices'][0]['message']['content']
            
            # Parse the JSON response from the LLM
            prediction = json.loads(prediction_text)
            logger.info(f"Successfully received weather report: {json.dumps(prediction)[:100]}...")
            return prediction
        else:
            logger.error(f"API request failed with status code {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error in call_prediction_api: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

@with_db_connection
def generate_weather_prediction(db=None, date=None, force_cache_overwrite=False, hours_to_analyze=None):
    """
    Generate weather reports using LLM based on recent measurements
    Note: Despite the function name, this now generates weather reports instead of predictions
    to maintain compatibility with downstream services.
    
    Args:
        db: Database connection
        date: Specific date to generate report for (format: YYYY-MM-DD)
        force_cache_overwrite: Whether to force regeneration of report even if a recent one exists
        hours_to_analyze: Number of hours of data to analyze (default from ANALYSIS_WINDOW_HOURS)
        
    Returns:
        The weather report document or None if failed
    """
    try:
        if db is None:
            db = get_database()        # Use provided date or default to current date
        current_date = date if date else datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Step 1: Check if we need a new prediction
        analysis_window = hours_to_analyze if hours_to_analyze is not None else ANALYSIS_WINDOW_HOURS

        if not force_cache_overwrite:
            recent_prediction = check_recent_prediction(db)
            if recent_prediction:
                logger.info(f"Recent prediction found from {recent_prediction['created_at']}")
                return recent_prediction
        
        # Step 2: Get measurements
        measurements = get_measurements(hours=analysis_window)
        if not measurements or len(measurements) == 0:
            logger.warning(f"No measurements found for the last {analysis_window} hours")

            try:
                # Build a fallback prediction that explains the data gap
                latest_measurement = db['measurements'].find_one(
                    sort=[('timestamp_ms', -1)]
                )
            except Exception as exc:
                logger.error(f"Error retrieving latest measurement for fallback reasoning: {exc}")

            latest_ts = None
            latest_location = None
            if latest_measurement:
                latest_ts = latest_measurement.get('timestamp_ms')
                latest_location = latest_measurement.get('tags', {}).get('location')

            logger.warning("Oli was here!")
            
            last_seen = None
            if isinstance(latest_ts, datetime):
                last_seen = latest_ts.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            elif latest_ts:
                try:
                    last_seen = str(latest_ts)
                except Exception:
                    last_seen = None

            reasoning_parts = [
                f"No recent weather measurements were found in the last {analysis_window} hours.",
            ]
            if last_seen:
                reasoning_parts.append(f"Last measurement seen at {last_seen}.")
            else:
                reasoning_parts.append("No prior measurement timestamp is available.")
            reasoning_parts.append("This report is based on missing data; please check the weather data source.")

            fallback_doc = {
                "date": current_date,
                "location": latest_location or "unknown",
                "created_at": datetime.now(timezone.utc),
                "prediction_12h": {},
                "prediction_24h": {},
                "reasoning": " ".join(reasoning_parts),
                "confidence": 0.0
            }

            # Store the fallback to ensure the chatbot can surface the data gap
            try:
                result = db['weather_predictions'].insert_one(fallback_doc)
                logger.info(f"Stored no-data weather report for {current_date} with ID: {result.inserted_id}")
            except Exception as insert_exc:
                logger.warning(f"Failed to store no-data report: {insert_exc}")

            return fallback_doc
            
        logger.info(f"Retrieved {len(measurements)} measurements for analysis")
        
        # Get location from the first measurement
        location = measurements[0]['tags'].get('location', 'unknown')
        logger.info(f"Processing data for location: {location}")
        
        # Step 3: Create weather summary
        weather_summary = prepare_weather_summary(measurements)
        if not weather_summary:
            logger.error("Failed to create weather summary from measurements")
            return None
            
        # Step 4: Analyze trends
        trend_analysis = analyze_weather_trends(measurements)
        if not trend_analysis:
            logger.warning("Could not analyze trends, will proceed without trend data")

        # Step 4b: Analyze precipitation
        precipitation_info = analyze_precipitation(measurements)
        
        # Step 4c: Analyze lux anomalies
        lux_info = analyze_lux_anomaly(measurements)
        if lux_info.get('anomalous'):
            logger.info(f"Lux anomaly detected: {lux_info['reason']}")
        
        # Step 4d: Fetch NWS alerts and forecast
        logger.info("Fetching NWS alerts and forecast data...")
        nws_data = get_nws_data()
        if nws_data.get('alerts'):
            logger.info(f"Found {len(nws_data['alerts'])} active NWS alerts")
        else:
            logger.info("No active NWS alerts")
            
        # Step 5: Prepare data for the LLM
        prompt_data = {
            "date": current_date,
            "location": location,
            "summary": weather_summary,
            "recent_trends": trend_analysis,
            "analysis_window_hours": analysis_window,
            "precipitation": precipitation_info,
            "lux_anomaly": lux_info,
            "nws_data": nws_data
        }
        
        # Step 6: Call the LLM API
        prediction_result = call_prediction_api(prompt_data)
        if not prediction_result:
            logger.error("Failed to get weather report from LLM API")
            return None        # Step 7: Store the prediction
        prediction_doc = {
            "date": current_date,
            "location": location,
            "created_at": datetime.now(timezone.utc),
        }
        
        # Handle case where prediction_result might be a list instead of a dictionary
        if isinstance(prediction_result, list):
            logger.warning("Prediction result returned as a list, attempting to use first item")
            if prediction_result and isinstance(prediction_result[0], dict):
                prediction_result = prediction_result[0]
            else:
                logger.error("Cannot extract valid prediction from list result")
                return None
        
        # Now safely extract values from prediction_result dictionary
        reasoning_text = prediction_result.get('reasoning', "")
        if precipitation_info.get('detected'):
            precip_window = analysis_window
            note = f"Rain/precipitation was detected in the last {precip_window} hours."
            if reasoning_text:
                reasoning_text = reasoning_text.rstrip() + " " + note
            else:
                reasoning_text = note

        prediction_doc.update({
            "prediction_12h": prediction_result.get('prediction_12h', {}),
            "prediction_24h": prediction_result.get('prediction_24h', {}),
            "reasoning": reasoning_text,
            "confidence": prediction_result.get('confidence', 0.0)
        })

        memory_key_points = []
        if trend_analysis:
            for parameter, values in trend_analysis.items():
                direction = values.get('direction', 'stable')
                rate = values.get('rate_per_hour', 0)
                memory_key_points.append(f"{parameter} trend {direction} ({rate:.2f}/hour)")

        if precipitation_info.get('detected'):
            memory_key_points.append(
                f"precipitation detected; positive samples={precipitation_info.get('positive_samples', 0)}"
            )

        if lux_info.get('anomalous'):
            memory_key_points.append(f"lux anomaly: {lux_info.get('reason')}")

        if nws_data.get('alerts'):
            memory_key_points.append(f"active NWS alerts count={len(nws_data.get('alerts', []))}")
        else:
            memory_key_points.append("no active NWS alerts")

        try:
            append_memory_entry({
                "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                "location": location,
                "analysis_window_hours": analysis_window,
                "confidence": prediction_doc.get("confidence", 0.0),
                "reasoning": prediction_doc.get("reasoning", ""),
                "key_points": memory_key_points,
            })

            if should_compact_memory():
                compacted = compact_memory_file()
                if compacted:
                    logger.info("WeatherBot memory compacted successfully")
                else:
                    logger.warning("WeatherBot memory compaction was triggered but made no changes")
        except Exception as memory_error:
            logger.warning(f"Memory write/compaction failed but weather report generation will continue: {memory_error}")

          # Debug: Log the type of created_at before insertion
        logger.info(f"About to insert prediction with created_at type: {type(prediction_doc['created_at'])}, value: {prediction_doc['created_at']}")
        logger.info(f"Prediction doc before insertion: {prediction_doc}")
        
        # Insert the prediction
        result = db['weather_predictions'].insert_one(prediction_doc)
        logger.info(f"Stored new weather report for {current_date} with ID: {result.inserted_id}")
        
        # Debug: Retrieve the document back to check how it's stored
        stored_doc = db['weather_predictions'].find_one({"_id": result.inserted_id})
        logger.info(f"Retrieved prediction created_at type: {type(stored_doc['created_at'])}, value: {stored_doc['created_at']}")
        logger.info(f"Full stored document: {stored_doc}")
            
        return prediction_doc
    except Exception as e:
        logger.error(f"Error in generate_weather_prediction: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

@with_db_connection
def check_recent_prediction(db=None, hours=12):
    """
    Check if we have a prediction from the last N hours
    
    Args:
        db: Database connection (optional)
        hours: Number of hours to look back (default: 12)
        
    Returns:
        The most recent prediction document or None if not found
    """
    try:
        if db is None:
            db = get_database()
            
        hours_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        recent_prediction = db['feather_predictions'].find_one(
            {'created_at': {'$gte': hours_ago}},
            sort=[('created_at', -1)]
        )
        
        return recent_prediction
    except Exception as e:
        logger.error(f"Error checking for recent prediction: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

@with_db_connection
def get_measurements(hours=None, location=None, db=None):
    """
    Get the last N hours of measurements
    
    Args:
        hours: Number of hours of data to retrieve (default from ANALYSIS_WINDOW_HOURS)
        location: Location to filter by (optional)
        db: Database connection (optional)
        
    Returns:
        List of measurement documents
    """
    try:
        if db is None:
            db = get_database()
            
        window_hours = hours if hours is not None else ANALYSIS_WINDOW_HOURS
        hours_ago = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        
        query = {'timestamp_ms': {'$gte': hours_ago}}
        if location:
            query['tags.location'] = location

        projection = {
            'fields': 1,
            'tags': 1,
            'timestamp_ms': 1
        }
        
        measurements = list(db['measurements'].find(
            query,
            projection=projection,
            sort=[('timestamp_ms', 1)]
        ))
        
        return measurements
    except Exception as e:
        logger.error(f"Error retrieving measurements: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def _extract_numeric_value(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict) and 'avg' in value and isinstance(value['avg'], (int, float)):
        return float(value['avg'])
    return None

def _extract_numeric_stats(value):
    if isinstance(value, (int, float)):
        val = float(value)
        return val, val, val
    if isinstance(value, dict):
        min_val = value.get('min')
        max_val = value.get('max')
        avg_val = value.get('avg')
        return (
            float(min_val) if isinstance(min_val, (int, float)) else None,
            float(max_val) if isinstance(max_val, (int, float)) else None,
            float(avg_val) if isinstance(avg_val, (int, float)) else None,
        )
    return None, None, None

def analyze_weather_trends(measurements):
    """
    Analyze measurements to extract trends
    
    Args:
        measurements: List of measurement documents
        
    Returns:
        Dictionary of trend analyses by parameter
    """
    if not measurements or len(measurements) < 2:
        logger.warning("Not enough measurements to analyze trends")
        return {}
        
    # Group by weather parameter
    param_values = {
        'temperature': [],
        'humidity': [],
        'pressure': [],
        'wind_speed': []
    }
    
    # Extract values for each measurement
    for m in measurements:
        timestamp = m.get('timestamp_ms') or m.get('timestamp')
        fields = m.get('fields', {})
        
        for param in param_values.keys():
            if param in fields:
                numeric_value = _extract_numeric_value(fields[param])
                if numeric_value is not None:
                    param_values[param].append({
                        'timestamp': timestamp,
                        'value': numeric_value
                    })
    
    # Calculate trends (direction and rate of change)
    trend_analysis = {}
    for param, values in param_values.items():
        if len(values) >= 2:
            first_value = values[0]['value']
            last_value = values[-1]['value']
            first_ts = values[0]['timestamp']
            last_ts = values[-1]['timestamp']

            hours_diff = None
            if isinstance(first_ts, datetime) and isinstance(last_ts, datetime):
                hours_diff = (last_ts - first_ts).total_seconds() / 3600

            if not hours_diff or hours_diff <= 0:
                hours_diff = max(len(values) - 1, 1)
            
            # Overall change
            change = last_value - first_value
            
            # Hourly rate of change
            rate_per_hour = change / hours_diff
            
            trend_analysis[param] = {
                'change': change,
                'rate_per_hour': rate_per_hour,
                'direction': 'rising' if change > 0 else 'falling' if change < 0 else 'stable'
            }
    
    return trend_analysis

def analyze_precipitation(measurements):
    """
    Analyze measurements for precipitation indicators.

    Returns:
        dict with detection status and field metadata
    """
    if not measurements:
        return {"detected": False, "fields": [], "positive_samples": 0}

    precip_fields = set()
    positive_samples = 0

    def _numeric_values(value):
        if isinstance(value, (int, float)):
            return [float(value)]
        if isinstance(value, dict):
            vals = []
            for v in value.values():
                if isinstance(v, (int, float)):
                    vals.append(float(v))
            return vals
        return []

    for m in measurements:
        fields = m.get('fields', {})
        for key, val in fields.items():
            if any(k in key.lower() for k in ['rain', 'precip']):
                precip_fields.add(key)
                values = _numeric_values(val)
                if any(v > 0 for v in values):
                    positive_samples += 1

    detected = positive_samples > 0
    return {
        "detected": detected,
        "fields": sorted(list(precip_fields)),
        "positive_samples": positive_samples
    }

def analyze_lux_anomaly(measurements):
    """
    Analyze lux/light levels to detect anomalous conditions.
    Only flags interesting cases like unusually dark during daytime or bright at night.
    
    Returns:
        dict with anomaly detection status and context
    """
    if not measurements:
        return {"anomalous": False, "reason": None, "lux_avg": None}
    
    # Extract lux values and timestamps
    lux_readings = []
    for m in measurements:
        timestamp = m.get('timestamp_ms')
        fields = m.get('fields', {})
        
        # Look for lux or light level fields
        for key, val in fields.items():
            if any(k in key.lower() for k in ['lux', 'light', 'illuminance']):
                numeric_val = _extract_numeric_value(val)
                if numeric_val is not None and timestamp:
                    lux_readings.append({
                        'value': numeric_val,
                        'timestamp': timestamp
                    })
    
    if not lux_readings:
        return {"anomalous": False, "reason": None, "lux_avg": None}
    
    # Calculate average lux
    avg_lux = sum(r['value'] for r in lux_readings) / len(lux_readings)
    
    # Get local time for the most recent reading
    try:
        tz = ZoneInfo(LOCAL_TIMEZONE)
        latest_time = lux_readings[-1]['timestamp']
        if isinstance(latest_time, datetime):
            local_dt = latest_time.astimezone(tz)
        else:
            local_dt = datetime.now(tz)
        
        hour = local_dt.hour
        
        # Define thresholds based on time of day
        # Daytime hours: 8am - 6pm (should be bright)
        # Night hours: 10pm - 6am (should be dark)
        # Twilight hours: 6am-8am, 6pm-10pm (variable)
        
        is_daytime = 8 <= hour < 18
        is_nighttime = hour >= 22 or hour < 6
        is_twilight = not (is_daytime or is_nighttime)
        
        # Thresholds (approximate lux levels):
        # Full daylight: 10,000+ lux
        # Overcast day: 1,000-10,000 lux  
        # Very dark/storm: <500 lux during day
        # Indoor/dark: <100 lux
        # Night: typically <10 lux
        
        anomalous = False
        reason = None
        
        if is_daytime:
            if avg_lux < 500:
                anomalous = True
                if avg_lux < 100:
                    reason = f"Unusually dark for midday (avg {avg_lux:.1f} lux at {hour}:00) - typical indoor lighting levels during daytime hours"
                else:
                    reason = f"Significantly reduced daylight (avg {avg_lux:.1f} lux at {hour}:00) - possible heavy cloud cover or storm conditions"
        elif is_nighttime:
            if avg_lux > 500:
                anomalous = True
                reason = f"Unusually bright for nighttime (avg {avg_lux:.1f} lux at {hour}:00)"
        # Twilight hours - only flag extreme cases
        elif is_twilight:
            if avg_lux < 50 and hour < 20:  # Very dark during early evening
                anomalous = True
                reason = f"Darker than expected for {hour}:00 (avg {avg_lux:.1f} lux)"
        
        return {
            "anomalous": anomalous,
            "reason": reason,
            "lux_avg": avg_lux,
            "hour": hour,
            "time_period": "daytime" if is_daytime else "nighttime" if is_nighttime else "twilight"
        }
        
    except Exception as e:
        logger.warning(f"Error analyzing lux anomaly: {e}")
        return {"anomalous": False, "reason": None, "lux_avg": avg_lux}

def prepare_weather_summary(measurements):
    """
    Create a summary of weather conditions from measurements
    
    Args:
        measurements: List of measurement documents
        
    Returns:
        Dictionary with summary statistics for each parameter
    """
    if not measurements:
        logger.warning("No measurements provided for summary")
        return None
        
    summary = {
        'temperature': {'min': float('inf'), 'max': float('-inf'), 'avg': 0},
        'humidity': {'min': float('inf'), 'max': float('-inf'), 'avg': 0},
        'pressure': {'min': float('inf'), 'max': float('-inf'), 'avg': 0},
        'wind_speed': {'min': float('inf'), 'max': float('-inf'), 'avg': 0}
    }
    
    # Initialize counters for calculating averages
    count = {param: 0 for param in summary.keys()}
    
    for m in measurements:
        fields = m.get('fields', {})
        
        for param in summary.keys():
            if param in fields:
                min_val, max_val, avg_val = _extract_numeric_stats(fields[param])

                # Get the minimum value
                if min_val is not None and min_val < summary[param]['min']:
                    summary[param]['min'] = min_val
                    
                # Get the maximum value
                if max_val is not None and max_val > summary[param]['max']:
                    summary[param]['max'] = max_val
                    
                # Accumulate average values for later calculation
                if avg_val is not None:
                    summary[param]['avg'] += avg_val
                    count[param] += 1
    
    # Calculate final averages
    for param in summary.keys():
        if count[param] > 0:
            summary[param]['avg'] /= count[param]
        
        # Handle cases where min/max weren't found
        if summary[param]['min'] == float('inf'):
            summary[param]['min'] = summary[param]['avg']
        if summary[param]['max'] == float('-inf'):
            summary[param]['max'] = summary[param]['avg']
    
    return summary