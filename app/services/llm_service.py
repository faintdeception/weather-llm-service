#!/usr/bin/env python3
"""
LLM Service for Weather Predictions

This module handles all interactions with the LLM API for generating
weather predictions based on collected weather data.
"""
import os
import sys
import json
import logging
import traceback
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
from app.database.connection import get_database, with_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("llm-service")

@with_db_connection
def get_daily_report(date=None):
    """
    Retrieve a daily report from the database
    
    Args:
        date: Date string in YYYY-MM-DD format, defaults to yesterday
        
    Returns:
        The daily report document or None if not found
    """
    try:
        db = get_database()
        
        if date is None:
            # Default to yesterday
            yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
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
    Call an external LLM API to generate weather predictions
    
    Args:
        weather_data: Dictionary with weather summary data
        
    Returns:
        Dictionary with prediction results or None if failed
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
        
        # Construct the prompt for the LLM
        prompt = f"""
Based on the following weather data from {weather_data['location']} on {weather_data['date']}, please provide:
1. A 12-hour weather prediction
2. A 24-hour weather prediction
3. Your reasoning based on the trends
4. A confidence score (0.0-1.0)

Current weather summary:
Temperature: Min {weather_data['summary']['temperature']['min']:.2f}°C, Max {weather_data['summary']['temperature']['max']:.2f}°C, Avg {weather_data['summary']['temperature']['avg']:.2f}°C
Humidity: Min {weather_data['summary']['humidity']['min']:.2f}%, Max {weather_data['summary']['humidity']['max']:.2f}%, Avg {weather_data['summary']['humidity']['avg']:.2f}%
Pressure: Min {weather_data['summary']['pressure']['min']:.2f} hPa, Max {weather_data['summary']['pressure']['max']:.2f} hPa, Avg {weather_data['summary']['pressure']['avg']:.2f} hPa
Wind Speed: Min {weather_data['summary']['wind_speed']['min']:.2f} mph, Max {weather_data['summary']['wind_speed']['max']:.2f} mph, Avg {weather_data['summary']['wind_speed']['avg']:.2f} mph
"""
        
        # Add trend data if available
        if weather_data.get('recent_trends'):
            prompt += "\nRecent trends (over last 12 hours):"
            for param, trend_data in weather_data['recent_trends'].items():
                direction = trend_data.get('direction', 'stable')
                change = trend_data.get('change', 0)
                rate = trend_data.get('rate_per_hour', 0)
                
                prompt += f"\n{param.capitalize()}: {direction}, Change: {change:.2f}, Rate: {rate:.2f}/hour"
        
        prompt += "\n\nPlease explain your reasoning using standard parlance, make the weather report easy for humans to understand and apply."
        prompt += "\n\nPlease format your response as JSON with keys: prediction_12h, prediction_24h, reasoning, confidence"
        
        # Log the API request (without the key)
        logger.info(f"Calling LLM API: {api_url}")
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
                "messages": [
                    {"role": "system", "content": "You are a weather forecasting assistant that analyzes weather data and provides predictions in JSON format."},
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
            logger.info(f"Successfully received prediction: {json.dumps(prediction)[:100]}...")
            return prediction
        else:
            logger.error(f"API request failed with status code {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error in call_prediction_api: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

@with_db_connection
def generate_weather_prediction(db=None, date=None, force=False, hours_to_analyze=12):
    """
    Generate weather predictions using LLM based on recent hourly measurements
    
    Args:
        db: Database connection
        date: Specific date to generate prediction for (format: YYYY-MM-DD)
        force: Whether to force regeneration of prediction even if a recent one exists
        hours_to_analyze: Number of hours of data to analyze (default: 12)
        
    Returns:
        The prediction document or None if failed
    """
    try:
        if db is None:
            db = get_database()
        
        # Use provided date or default to current date
        current_date = date if date else datetime.now().strftime('%Y-%m-%d')
        
        # Step 1: Check if we need a new prediction
        if not force:
            recent_prediction = check_recent_prediction(db)
            if recent_prediction:
                logger.info(f"Recent prediction found from {recent_prediction['created_at']}")
                return recent_prediction
        
        # Step 2: Get hourly measurements
        hourly_data = get_hourly_measurements(hours=hours_to_analyze)
        if not hourly_data or len(hourly_data) == 0:
            logger.error(f"No hourly measurements found for the last {hours_to_analyze} hours")
            return None
            
        logger.info(f"Retrieved {len(hourly_data)} hours of weather data")
        
        # Get location from the first measurement
        location = hourly_data[0]['tags'].get('location', 'unknown')
        logger.info(f"Processing data for location: {location}")
        
        # Step 3: Create weather summary
        weather_summary = prepare_weather_summary(hourly_data)
        if not weather_summary:
            logger.error("Failed to create weather summary from measurements")
            return None
            
        # Step 4: Analyze trends
        trend_analysis = analyze_weather_trends(hourly_data)
        if not trend_analysis:
            logger.warning("Could not analyze trends, will proceed without trend data")
            
        # Step 5: Prepare data for the LLM
        prompt_data = {
            "date": current_date,
            "location": location,
            "summary": weather_summary,
            "recent_trends": trend_analysis
        }
        
        # Step 6: Call the LLM API
        prediction_result = call_prediction_api(prompt_data)
        if not prediction_result:
            logger.error("Failed to get prediction from LLM API")
            return None
          # Step 7: Store the prediction
        prediction_doc = {
            "date": current_date,
            "location": location,
            "created_at": datetime.utcnow(),
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
        prediction_doc.update({
            "prediction_12h": prediction_result.get('prediction_12h', {}),
            "prediction_24h": prediction_result.get('prediction_24h', {}),
            "reasoning": prediction_result.get('reasoning', ""),
            "confidence": prediction_result.get('confidence', 0.0)
        })
        
        # Insert the prediction
        db['weather_predictions'].insert_one(prediction_doc)
        logger.info(f"Stored new weather prediction for {current_date}")
            
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
            
        hours_ago = datetime.utcnow() - timedelta(hours=hours)
        
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
def get_hourly_measurements(hours=6, location=None, db=None):
    """
    Get the last N hours of measurements
    
    Args:
        hours: Number of hours of data to retrieve
        location: Location to filter by (optional)
        db: Database connection (optional)
        
    Returns:
        List of hourly measurement documents
    """
    try:
        if db is None:
            db = get_database()
            
        hours_ago = datetime.utcnow() - timedelta(hours=hours)
        
        query = {'timestamp_ms': {'$gte': hours_ago}}
        if location:
            query['tags.location'] = location
        
        measurements = list(db['hourly_measurements'].find(
            query,
            sort=[('timestamp_ms', 1)]
        ))
        
        return measurements
    except Exception as e:
        logger.error(f"Error retrieving hourly measurements: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def analyze_weather_trends(measurements):
    """
    Analyze measurements to extract trends
    
    Args:
        measurements: List of hourly measurement documents
        
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
    
    # Extract values for each hour
    for m in measurements:
        timestamp = m['timestamp_ms']
        fields = m['fields']
        
        for param in param_values.keys():
            if param in fields and 'avg' in fields[param]:
                param_values[param].append({
                    'timestamp': timestamp,
                    'value': fields[param]['avg']
                })
    
    # Calculate trends (direction and rate of change)
    trend_analysis = {}
    for param, values in param_values.items():
        if len(values) >= 2:
            first_value = values[0]['value']
            last_value = values[-1]['value']
            hours_diff = len(values)
            
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

def prepare_weather_summary(measurements):
    """
    Create a summary of weather conditions from hourly measurements
    
    Args:
        measurements: List of hourly measurement documents
        
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
        fields = m['fields']
        
        for param in summary.keys():
            if param in fields:
                # Get the minimum value
                if 'min' in fields[param] and fields[param]['min'] < summary[param]['min']:
                    summary[param]['min'] = fields[param]['min']
                    
                # Get the maximum value
                if 'max' in fields[param] and fields[param]['max'] > summary[param]['max']:
                    summary[param]['max'] = fields[param]['max']
                    
                # Accumulate average values for later calculation
                if 'avg' in fields[param]:
                    summary[param]['avg'] += fields[param]['avg']
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