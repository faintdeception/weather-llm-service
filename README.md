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

## NWS Integration

The service integrates with the [National Weather Service API](https://www.weather.gov/documentation/services-web-api) to provide:

- **Active Alerts**: Winter storm warnings, cold weather advisories, severe weather alerts
- **Official Forecasts**: Detailed predictions for the next 24-48 hours
- **Safety Instructions**: NWS-provided guidance for severe weather events

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
MONGODB_CONNECTION_STRING=your_mongodb_connection
LOCAL_TIMEZONE=America/New_York
ANALYSIS_WINDOW_HOURS=3
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

## Architecture

- `app/services/llm_service.py` - Core LLM integration and report generation
- `app/services/nws_service.py` - National Weather Service API client
- `app/services/scheduled_task.py` - Main scheduled job entry point
- `app/database/connection.py` - MongoDB connection management
- `run_job.ps1` - PowerShell wrapper for scheduled execution

