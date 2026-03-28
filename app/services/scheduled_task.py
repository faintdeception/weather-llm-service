#!/usr/bin/env python3
"""
Scheduled Task for Weather Reports

This script runs every 15 minutes via Windows Task Scheduler to generate weather reports
based on recent data. Reports include NWS alerts and forecasts for enhanced severe weather
detection. Reports are stored in prediction format for downstream compatibility.
"""
import os
import logging
import sys
from pathlib import Path
from app.services.llm_service import (
    generate_weather_prediction,
    get_measurements,
    ANALYSIS_WINDOW_HOURS
)
from app.services.memory_service import get_memory_settings

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


def _log_memory_status():
    """Log memory thread size and run entry count for observability."""
    try:
        settings = get_memory_settings()
        memory_path = Path(settings["memory_path"])

        if not memory_path.exists():
            logger.info(f"WeatherBot memory file not found yet at {memory_path}")
            return

        size_bytes = memory_path.stat().st_size
        max_file_bytes = settings["max_file_bytes"]
        compact_at_ratio = settings["compact_at_ratio"]
        target_ratio = settings["target_ratio"]
        compact_at_bytes = int(max_file_bytes * compact_at_ratio)
        target_bytes = int(max_file_bytes * target_ratio)

        run_count = 0
        with memory_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.startswith("## Run 20"):
                    run_count += 1

        logger.info(
            "WeatherBot memory status: path=%s size=%d bytes runs=%d compact_at=%d bytes target=%d bytes max=%d bytes",
            memory_path,
            size_bytes,
            run_count,
            compact_at_bytes,
            target_bytes,
            max_file_bytes,
        )
    except Exception as exc:
        logger.warning(f"Unable to log WeatherBot memory status: {exc}")

def main():
    """Main function to run the scheduled task"""
    logger.info("Starting scheduled weather report generation task")
    logger.info(f"stdout encoding: {sys.stdout.encoding}, stderr encoding: {sys.stderr.encoding}")
    
    try:
        # Get the current time for logging
        from datetime import datetime, timezone
        current_time = datetime.now(timezone.utc)
        logger.info(f"Running scheduled job at {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # Use the configured analysis window (env-driven, default 3h)
        hours_to_analyze = ANALYSIS_WINDOW_HOURS
        
        # Get measurements for the configured window
        measurements = get_measurements(hours=hours_to_analyze)
        if not measurements or len(measurements) == 0:
            logger.error(f"No measurements found for the last {hours_to_analyze} hours")
            # sys.exit(1)
            
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
    _log_memory_status()
    
if __name__ == "__main__":
    main()