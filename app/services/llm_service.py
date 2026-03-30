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
TWILIGHT_BUFFER_MINUTES = int(os.getenv("TWILIGHT_BUFFER_MINUTES", "45"))
MAX_SOLAR_DAY_OFFSET = int(os.getenv("MAX_SOLAR_DAY_OFFSET", "1"))
SOLAR_MENTION_WINDOW_MINUTES = int(os.getenv("SOLAR_MENTION_WINDOW_MINUTES", "60"))


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


def _collect_reporting_issues(weather_data):
    """Collect reliability issues that should be disclosed in the final narrative."""
    issues = []

    nws_data = weather_data.get("nws_data") or {}
    forecast = nws_data.get("forecast")
    if forecast is None:
        issues.append("NWS forecast data unavailable")
    elif not forecast.get("periods"):
        issues.append("NWS forecast periods missing")

    lux_info = weather_data.get("lux_anomaly") or {}
    source = lux_info.get("classification_source")
    fallback_reason = lux_info.get("fallback_reason")

    if source == "hour_fallback":
        if fallback_reason:
            issues.append(f"Daylight classifier fallback active: {fallback_reason}")
        else:
            issues.append("Daylight classifier fallback active")

    return issues


def _fallback_confusion_addendum(issues):
    issue_text = "; ".join(issues)
    return (
        "Noticed Some Hard Failures that May Impact Reporting: "
        f"I'm a little confused because {issue_text}. "
        "I still used the best available data, but confidence may be slightly reduced."
    )


def _request_confusion_addendum(api_url, api_key, model_name, issues, prediction, base_temperature):
    """Ask the LLM for a short reliability addendum after the main report is generated."""
    try:
        response = requests.post(
            api_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model_name,
                "temperature": min(max(base_temperature, 0.0), 0.6),
                "messages": [
                    {
                        "role": "system",
                        "content": "You are WeatherBot. Produce a brief reliability disclaimer addendum in-character.",
                    },
                    {
                        "role": "user",
                        "content": (
                            "Given this weather report reasoning and issue list, return JSON with exactly one key 'addendum'. "
                            "The addendum must start with 'Noticed Some Hard Failures that May Impact Reporting:' and be 1-2 sentences. "
                            "Mention that WeatherBot is a little confused while staying helpful. "
                            f"Issues: {issues}. "
                            f"Existing reasoning: {prediction.get('reasoning', '')}"
                        ),
                    },
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=45,
        )
        if response.status_code != 200:
            logger.warning(f"Confusion addendum request failed with status {response.status_code}")
            return None

        payload = response.json()
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        addendum_obj = json.loads(content) if content else {}
        addendum = addendum_obj.get("addendum")
        if isinstance(addendum, str) and addendum.strip():
            return addendum.strip()
        return None
    except Exception as exc:
        logger.warning(f"Failed to generate confusion addendum: {exc}")
        return None


def _build_solar_event_prompt_context(forecast, current_local_time):
    """Return nearby sunrise/sunset context only when event is within +/- window minutes."""
    context = {
        "include_solar_timing": False,
        "event_lines": [],
    }

    if not forecast or not isinstance(current_local_time, datetime):
        return context

    tz = current_local_time.tzinfo or timezone.utc

    for label, key in (("sunrise", "sunrise"), ("sunset", "sunset")):
        raw_value = forecast.get(key)
        event_dt = _parse_iso_datetime(raw_value)
        if not event_dt:
            continue

        if event_dt.tzinfo is None:
            event_dt = event_dt.replace(tzinfo=tz)

        event_local = event_dt.astimezone(tz)
        delta_minutes = (event_local - current_local_time).total_seconds() / 60.0
        if abs(delta_minutes) > SOLAR_MENTION_WINDOW_MINUTES:
            continue

        context["include_solar_timing"] = True
        rounded = int(round(abs(delta_minutes)))
        if rounded == 0:
            relation = "right now"
        elif delta_minutes > 0:
            relation = f"in about {rounded} minutes"
        else:
            relation = f"about {rounded} minutes ago"

        context["event_lines"].append(
            f"- Nearby {label}: {event_local.strftime('%Y-%m-%d %H:%M %Z')} ({relation})"
        )

    return context

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
        forecast_for_solar = (weather_data.get("nws_data") or {}).get("forecast") or {}
        solar_event_context = _build_solar_event_prompt_context(forecast_for_solar, current_local_time)
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

    Only mention sunrise/sunset timing when a "Nearby solar event context" section is provided. Otherwise, do not speculate about sunrise/sunset timing.

    
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
        if lux_anomaly:
            daylight_ctx = lux_anomaly.get('daylight_context') or {}
            source = lux_anomaly.get('classification_source', 'unknown')
            fallback_reason = lux_anomaly.get('fallback_reason')
            prompt += "\n\nDaylight-aware lux interpretation context:"
            prompt += f"\n- Classification source: {source}"
            prompt += f"\n- Time period at observation: {lux_anomaly.get('time_period', 'unknown')}"
            prompt += f"\n- Twilight buffer: {daylight_ctx.get('twilight_buffer_minutes', TWILIGHT_BUFFER_MINUTES)} minutes"
            if daylight_ctx.get('daylight_data_quality'):
                prompt += f"\n- Daylight data quality: {daylight_ctx.get('daylight_data_quality')}"
            if daylight_ctx.get('source_day_offset_days') is not None:
                prompt += f"\n- Source day offset: {daylight_ctx.get('source_day_offset_days')}"
            if fallback_reason:
                prompt += f"\n- Fallback reason: {fallback_reason}"

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
            prompt += format_forecast_for_prompt(
                nws_data.get('forecast'),
                include_solar_timing=solar_event_context.get("include_solar_timing", False),
            )
            if solar_event_context.get("event_lines"):
                prompt += "\nNearby solar event context:\n"
                prompt += "\n".join(solar_event_context["event_lines"])
                prompt += "\n"
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

            issues = _collect_reporting_issues(weather_data)
            if issues:
                addendum = _request_confusion_addendum(
                    api_url=api_url,
                    api_key=api_key,
                    model_name=model_name,
                    issues=issues,
                    prediction=prediction,
                    base_temperature=llm_temperature,
                )
                if not addendum:
                    addendum = _fallback_confusion_addendum(issues)

                existing_reasoning = prediction.get("reasoning", "")
                prediction["reasoning"] = (
                    f"{existing_reasoning.rstrip()}\n\n{addendum}" if existing_reasoning else addendum
                )

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

        # Step 4c: Fetch NWS alerts and forecast
        logger.info("Fetching NWS alerts and forecast data...")
        nws_data = get_nws_data()
        nws_data = _ensure_forecast_with_recent_fallback(
            nws_data=nws_data,
            db=db,
            max_previous=10,
        )

        if nws_data.get('alerts'):
            logger.info(f"Found {len(nws_data['alerts'])} active NWS alerts")
        else:
            logger.info("No active NWS alerts")

        # Cache the fetched NWS payload so downstream services can reuse it
        # without calling api.weather.gov directly.
        snapshot_id = store_nws_snapshot(
            nws_data=nws_data,
            db=db,
            report_date=current_date,
            location=location,
        )
        if snapshot_id:
            logger.info(f"Stored NWS snapshot with ID: {snapshot_id}")
        else:
            logger.warning("NWS snapshot was not stored")

        # Step 4d: Analyze lux anomalies with daylight context when available
        lux_info = analyze_lux_anomaly(measurements, nws_data=nws_data)
        if lux_info.get('anomalous'):
            logger.info(f"Lux anomaly detected: {lux_info['reason']}")
        if lux_info.get('classification_source') == 'hour_fallback':
            logger.info(
                "Lux anomaly classifier used hour fallback mode"
                + (f" ({lux_info.get('fallback_reason')})" if lux_info.get('fallback_reason') else "")
            )
            
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
        
        recent_prediction = db['weather_predictions'].find_one(
            {'created_at': {'$gte': hours_ago}},
            sort=[('created_at', -1)]
        )
        
        return recent_prediction
    except Exception as e:
        logger.error(f"Error checking for recent prediction: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


@with_db_connection
def store_nws_snapshot(nws_data, db=None, report_date=None, location=None):
    """
    Persist one fetched NWS payload so downstream consumers can use cached data.

    Args:
        nws_data: Payload returned by get_nws_data
        db: Database connection (optional)
        report_date: Report date in YYYY-MM-DD format
        location: Location identifier derived from local measurements

    Returns:
        Inserted snapshot ID as string, or None on failure
    """
    try:
        if db is None:
            db = get_database()

        if not isinstance(nws_data, dict):
            logger.warning("Skipping NWS snapshot persistence: payload is not a dict")
            return None

        snapshot_doc = {
            "report_date": report_date,
            "location": location,
            "created_at": datetime.now(timezone.utc),
            "fetched_at": nws_data.get("fetched_at"),
            "nws_data": {
                "alerts": nws_data.get("alerts", []),
                "forecast": nws_data.get("forecast"),
                "location": nws_data.get("location"),
            },
        }

        result = db["nws_snapshots"].insert_one(snapshot_doc)
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Error storing NWS snapshot: {str(e)}")
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


def _parse_iso_datetime(raw_value):
    """Parse API datetime strings with graceful support for non-ISO NWS formats."""
    if not raw_value or not isinstance(raw_value, str):
        return None

    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except Exception:
        pass

    # NWS astronomicalData is sometimes returned as MM/DD/YYYY HH:MM:SS.
    for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw_value, fmt)
        except Exception:
            continue

    return None


def _ensure_forecast_with_recent_fallback(nws_data, db, max_previous=10):
    """
    Ensure NWS payload has a forecast, falling back to recent cached snapshots.

    If a live NWS response contains ``forecast=None``, this reuses the most recent
    non-null forecast from up to ``max_previous`` cached snapshots. If no fallback
    exists (or DB lookup is unavailable), keep the null forecast and continue.
    """
    if not isinstance(nws_data, dict):
        logger.warning("NWS payload is invalid; expected a dictionary")
        return nws_data

    if nws_data.get("forecast") is not None:
        return nws_data

    if db is None:
        logger.warning(
            "NWS forecast is null and no database is available to check previous %s forecasts",
            max_previous,
        )
        return nws_data

    snapshots = list(
        db["nws_snapshots"].find(
            {},
            {"nws_data.forecast": 1, "created_at": 1},
            sort=[("created_at", -1)],
            limit=max_previous,
        )
    )

    for snapshot in snapshots:
        cached_forecast = (snapshot.get("nws_data") or {}).get("forecast")
        if cached_forecast is not None:
            nws_data["forecast"] = cached_forecast
            logger.warning(
                "NWS forecast was null; using cached forecast fallback from snapshot created_at=%s",
                snapshot.get("created_at"),
            )
            return nws_data

    logger.warning(
        "NWS forecast is null and all previous %s cached forecasts are null",
        max_previous,
    )
    return nws_data


def _build_daylight_windows(nws_data, reference_dt_local, tz):
    """
    Build sunrise/sunset and twilight windows from NWS forecast context.

    Returns:
        tuple: (windows dict or None, fallback_reason str or None)
    """
    if not nws_data:
        return None, "missing_nws_data"

    forecast = nws_data.get("forecast") or {}
    raw_sunrise = forecast.get("sunrise")
    raw_sunset = forecast.get("sunset")
    if not raw_sunrise or not raw_sunset:
        return None, "missing_sunrise_sunset"

    sunrise_dt = _parse_iso_datetime(raw_sunrise)
    sunset_dt = _parse_iso_datetime(raw_sunset)
    if not sunrise_dt or not sunset_dt:
        return None, "invalid_sunrise_sunset"

    if sunrise_dt.tzinfo is None:
        sunrise_dt = sunrise_dt.replace(tzinfo=tz)
    if sunset_dt.tzinfo is None:
        sunset_dt = sunset_dt.replace(tzinfo=tz)

    sunrise_local = sunrise_dt.astimezone(tz)
    sunset_local = sunset_dt.astimezone(tz)

    ref_date = reference_dt_local.date()
    sunrise_offset = (sunrise_local.date() - ref_date).days
    sunset_offset = (sunset_local.date() - ref_date).days

    if sunrise_offset != 0 or sunset_offset != 0:
        if sunrise_offset == sunset_offset and abs(sunrise_offset) <= MAX_SOLAR_DAY_OFFSET:
            # Reuse nearby-day solar clock times by shifting them to the target date.
            sunrise_local = sunrise_local - timedelta(days=sunrise_offset)
            sunset_local = sunset_local - timedelta(days=sunset_offset)
            daylight_data_quality = "adjacent_day_adjusted"
            source_day_offset_days = sunrise_offset
        else:
            return None, "sunrise_sunset_date_mismatch"
    else:
        daylight_data_quality = "same_day"
        source_day_offset_days = 0

    if sunrise_local >= sunset_local:
        return None, "invalid_daylight_order"

    buffer = timedelta(minutes=TWILIGHT_BUFFER_MINUTES)
    windows = {
        "sunrise": sunrise_local,
        "sunset": sunset_local,
        "dawn_start": sunrise_local - buffer,
        "dusk_end": sunset_local + buffer,
        "twilight_buffer_minutes": TWILIGHT_BUFFER_MINUTES,
        "daylight_data_quality": daylight_data_quality,
        "source_day_offset_days": source_day_offset_days,
    }
    return windows, None


def _classify_time_period_from_windows(local_dt, windows):
    """Classify local time into night/twilight/daylight based on solar windows."""
    if local_dt < windows["dawn_start"]:
        return "night"
    if windows["dawn_start"] <= local_dt < windows["sunrise"]:
        return "twilight"
    if windows["sunrise"] <= local_dt < windows["sunset"]:
        return "daylight"
    if windows["sunset"] <= local_dt <= windows["dusk_end"]:
        return "twilight"
    return "night"


def _classify_time_period_fallback(local_dt):
    """Legacy hour-based fallback for cases where sunrise/sunset data is unavailable."""
    hour = local_dt.hour
    is_daytime = 8 <= hour < 18
    is_nighttime = hour >= 22 or hour < 6
    if is_daytime:
        return "daylight"
    if is_nighttime:
        return "night"
    return "twilight"

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

def analyze_lux_anomaly(measurements, nws_data=None):
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
        latest_time = max(
            (r['timestamp'] for r in lux_readings if isinstance(r.get('timestamp'), datetime)),
            default=None,
        )
        if isinstance(latest_time, datetime):
            # Mongo clients without tz-aware decoding can return naive UTC datetimes.
            # Interpret naive values as UTC before converting to local time.
            if latest_time.tzinfo is None:
                latest_time = latest_time.replace(tzinfo=timezone.utc)
            local_dt = latest_time.astimezone(tz)
        else:
            local_dt = datetime.now(tz)

        hour = local_dt.hour
        windows, fallback_reason = _build_daylight_windows(nws_data, local_dt, tz)
        if windows:
            time_period = _classify_time_period_from_windows(local_dt, windows)
            source = "nws_solar_adjusted" if windows.get("daylight_data_quality") == "adjacent_day_adjusted" else "nws_solar"
        else:
            time_period = _classify_time_period_fallback(local_dt)
            source = "hour_fallback"

        anomalous = False
        reason = None

        if time_period == "daylight":
            if avg_lux < 500:
                anomalous = True
                if avg_lux < 100:
                    reason = f"Unusually dark for daylight hours (avg {avg_lux:.1f} lux at {hour}:00) - typical indoor lighting levels"
                else:
                    reason = f"Significantly reduced daylight (avg {avg_lux:.1f} lux at {hour}:00) - possible heavy cloud cover or storm conditions"
        elif time_period == "night":
            if avg_lux > 500:
                anomalous = True
                reason = f"Unusually bright for nighttime (avg {avg_lux:.1f} lux at {hour}:00)"
        else:
            # Twilight remains permissive; only flag clearly abnormal darkness/brightness.
            if avg_lux < 10:
                anomalous = True
                reason = f"Darker than expected for twilight (avg {avg_lux:.1f} lux at {hour}:00)"
            elif avg_lux > 20000:
                anomalous = True
                reason = f"Exceptionally bright for twilight (avg {avg_lux:.1f} lux at {hour}:00)"

        if windows:
            daylight_context = {
                "sunrise": windows["sunrise"].isoformat(),
                "sunset": windows["sunset"].isoformat(),
                "dawn_start": windows["dawn_start"].isoformat(),
                "dusk_end": windows["dusk_end"].isoformat(),
                "twilight_buffer_minutes": windows["twilight_buffer_minutes"],
                "daylight_data_quality": windows.get("daylight_data_quality"),
                "source_day_offset_days": windows.get("source_day_offset_days"),
            }
        else:
            daylight_context = {
                "sunrise": None,
                "sunset": None,
                "dawn_start": None,
                "dusk_end": None,
                "twilight_buffer_minutes": TWILIGHT_BUFFER_MINUTES,
                "daylight_data_quality": "fallback",
                "source_day_offset_days": None,
            }
        
        return {
            "anomalous": anomalous,
            "reason": reason,
            "lux_avg": avg_lux,
            "hour": hour,
            "time_period": time_period,
            "classification_source": source,
            "fallback_reason": fallback_reason,
            "daylight_context": daylight_context,
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