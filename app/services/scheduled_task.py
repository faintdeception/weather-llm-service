#!/usr/bin/env python3
"""
Scheduled Task for Weather Reports

This script is designed to run at 6am and 6pm to generate weather reports
based on the previous 12 hours of data. Reports are stored in prediction format
for downstream compatibility.
"""
import os
import logging
import sys
from app.services.llm_service import (
    generate_weather_prediction,
    get_measurements,
    ANALYSIS_WINDOW_HOURS
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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
logger = logging.getLogger("scheduled-task")

def main():
    """Main function to run the scheduled task"""
    logger.info("Starting scheduled weather report generation task")
    logger.info(f"stdout encoding: {sys.stdout.encoding}, stderr encoding: {sys.stderr.encoding}")
    
    try:        # Get the current hour to include in the log for clarity on which run this is (6am or 6pm)
        from datetime import datetime, timezone
        current_hour = datetime.now(timezone.utc).hour
        am_pm = "AM" if current_hour < 12 else "PM"
        logger.info(f"Running scheduled job at {current_hour}:00 {am_pm} UTC")
        
        # Use the configured analysis window (env-driven, default 3h)
        hours_to_analyze = ANALYSIS_WINDOW_HOURS
        
        # Get measurements for the configured window
        measurements = get_measurements(hours=hours_to_analyze)
        if not measurements or len(measurements) == 0:
            logger.error(f"No measurements found for the last {hours_to_analyze} hours")
            sys.exit(1)
            
        logger.info(f"Retrieved {len(measurements)} measurements for analysis")
        
        # Force a new report regardless of when the last one was generated
        prediction = generate_weather_prediction(force_cache_overwrite=True, hours_to_analyze=hours_to_analyze)
        
        if prediction:
            logger.info(f"Successfully generated weather report for {prediction['date']}")
            logger.info(f"12-hour data: {prediction['prediction_12h']}")
            logger.info(f"24-hour data: {prediction['prediction_24h']}")
            logger.info(f"Confidence score: {prediction['confidence']}")
        else:
            logger.error("Failed to generate weather report")
            sys.exit(1)
            
    except Exception as e:
        logger.exception(f"Error running scheduled task: {str(e)}")
        sys.exit(1)
        
    logger.info(f"Scheduled weather report generation task completed successfully using {hours_to_analyze} hours of data")
    
if __name__ == "__main__":
    main()