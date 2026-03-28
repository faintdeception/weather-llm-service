# weather-llm-service

Weather reporting service that combines local sensor measurements with National Weather Service (NWS) data to generate comprehensive, AI-powered weather reports.

## Features

- **Local Weather Monitoring**: Collects and analyzes data from local weather sensors
- **NWS Integration**: Fetches active alerts and official forecasts from the National Weather Service
- **Severe Weather Detection**: Automatically includes warnings, watches, and advisories in reports
- **AI-Powered Reports**: Uses LLM to generate human-readable weather analysis with personality
- **Scheduled Execution**: Runs every 15 minutes via Windows Task Scheduler
- **Trend Analysis**: Tracks weather parameter changes over time
- **Precipitation Detection**: Identifies and reports rain/snow conditions
- **Forecast Comparison**: Compares NWS predictions against actual sensor observations
- **Daylight-Aware Lux Analysis**: Uses NWS sunrise/sunset data (instead of fixed clock hours) to evaluate unusual darkness/brightness
- **Reliability Addendum**: Adds an explicit "hard failures" note when fallback/partial-data conditions may impact confidence

## NWS Integration

The service integrates with the [National Weather Service API](https://www.weather.gov/documentation/services-web-api) to provide:

- **Active Alerts**: Winter storm warnings, cold weather advisories, severe weather alerts
- **Official Forecasts**: Detailed predictions for the next 24-48 hours
- **Solar Timing**: Sunrise and sunset values used for daylight-aware lux anomaly detection
- **Safety Instructions**: NWS-provided guidance for severe weather events
- **NWS Snapshot Caching**: Stores fetched NWS payloads in MongoDB so downstream services can reuse data without repeatedly calling NWS

Lux anomaly detection uses a twilight buffer around sunrise/sunset so WeatherBot is resilient to seasonal day-length changes and daylight saving time transitions.

If NWS sunrise/sunset arrives from an adjacent day (for example yesterday), WeatherBot reuses those solar times for the current day when the day offset is small. If hard failures or fallback paths are detected, the final reasoning includes a short addendum that calls out potential reporting impact.

### Cached NWS API Access

Each scheduled report run stores the fetched NWS payload in MongoDB collection `nws_snapshots`.
Snapshots are automatically retained for 7 days via a MongoDB TTL policy on `created_at`.

The weather service exposes cached NWS data via:

- `GET /api/predictions/nws/latest` - Most recent cached NWS payload
- `GET /api/predictions/nws/by-date/{date}` - Latest cached NWS payload for a report date (`YYYY-MM-DD`)

### Configuration

Location is set to Arlington, VA area (38.7692672, -77.0890822). To change:

Edit [app/services/nws_service.py](app/services/nws_service.py):
```python
NWS_LATITUDE = 38.7692672
NWS_LONGITUDE = -77.0890822
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables in `.env`:
```
LLM_API_KEY=your_api_key_here
LLM_TEMPERATURE=0.8
MONGO_URI=your_mongodb_connection
MONGO_DB=weather
LOCAL_TIMEZONE=America/New_York
ANALYSIS_WINDOW_HOURS=3
TWILIGHT_BUFFER_MINUTES=45
MAX_SOLAR_DAY_OFFSET=1
WEATHERBOT_MEMORY_FILE=logs/weatherbot_memory.md
WEATHERBOT_MEMORY_MAX_CONTEXT_CHARS=3500
WEATHERBOT_MEMORY_MAX_FILE_BYTES=262144
WEATHERBOT_MEMORY_KEEP_RECENT_RUNS=192
WEATHERBOT_MEMORY_COMPACT_AT_RATIO=0.9
WEATHERBOT_MEMORY_TARGET_RATIO=0.75
```

3. Run manually:
```powershell
.\run_job.ps1
```

## Scheduled Execution

The service is designed to run every 15 minutes via Windows Task Scheduler:

```powershell
# Set up Task Scheduler to run:
.\run_job.ps1
```

Logs are stored in `$env:ProgramData\weather-llm-service\logs\`

## Testing

Test the NWS integration:
```bash
python test_nws_integration.py
```

Test daylight-aware lux behavior:
```bash
python test_lux_awareness.py
```

Test cached NWS API endpoints:
```bash
python -m unittest tests.test_nws_cache_api
```

## Architecture

- `app/services/llm_service.py` - Core LLM integration and report generation
- `app/services/nws_service.py` - National Weather Service API client
- `app/services/scheduled_task.py` - Main scheduled job entry point
- `app/api/routes.py` - Prediction and cached NWS retrieval endpoints
- `app/database/connection.py` - MongoDB connection management
- `run_job.ps1` - PowerShell wrapper for scheduled execution

