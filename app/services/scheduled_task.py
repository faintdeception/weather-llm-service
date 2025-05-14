#!/usr/bin/env python3
"""
Scheduled Task for Weather Predictions

This script is designed to run at 6am and 6pm to generate weather predictions
based on the previous 12 hours of data.
"""
import os
import logging
import sys
from app.services.llm_service import generate_weather_prediction, get_hourly_measurements

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("scheduled-task")

def main():
    """Main function to run the scheduled task"""
    logger.info("Starting scheduled weather prediction task")
    
    try:
        # Get the current hour to include in the log for clarity on which run this is (6am or 6pm)
        from datetime import datetime
        current_hour = datetime.now().hour
        am_pm = "AM" if current_hour < 12 else "PM"
        logger.info(f"Running scheduled job at {current_hour}:00 {am_pm}")
        
        # Override the default 6 hours of data with 12 hours instead
        hours_to_analyze = 12
        
        # Get hourly measurements for the past 12 hours
        hourly_data = get_hourly_measurements(hours=hours_to_analyze)
        if not hourly_data or len(hourly_data) == 0:
            logger.error(f"No hourly measurements found for the last {hours_to_analyze} hours")
            sys.exit(1)
            
        logger.info(f"Retrieved {len(hourly_data)} hours of weather data")
        
        # Force a new prediction regardless of when the last one was generated
        prediction = generate_weather_prediction(force=True)
        
        if prediction:
            logger.info(f"Successfully generated prediction for {prediction['date']}")
            logger.info(f"12-hour prediction: {prediction['prediction_12h']}")
            logger.info(f"Confidence score: {prediction['confidence']}")
        else:
            logger.error("Failed to generate prediction")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error running scheduled task: {str(e)}")
        sys.exit(1)
        
    logger.info(f"Scheduled weather prediction task completed successfully using {hours_to_analyze} hours of data")
    
if __name__ == "__main__":
    main()