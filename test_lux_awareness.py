#!/usr/bin/env python3
"""
Test script for daylight-aware lux anomaly detection.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Simulate measurements with different lux scenarios
def create_test_measurement(lux_value, timestamp_utc):
    """Create a test measurement with specific lux value and timestamp."""
    return {
        'timestamp_ms': timestamp_utc,
        'fields': {
            'lux': lux_value,
            'temperature': 20.0,
            'humidity': 60.0
        },
        'tags': {
            'location': 'test_location'
        }
    }

# Import after creating test data structure
from app.services.llm_service import analyze_lux_anomaly


def make_nws_context_for_date(local_date, tz, sunrise_time=(6, 45), sunset_time=(19, 30)):
    """Build a minimal NWS payload containing sunrise/sunset for the target date."""
    sunrise_local = datetime(
        local_date.year,
        local_date.month,
        local_date.day,
        sunrise_time[0],
        sunrise_time[1],
        tzinfo=tz,
    )
    sunset_local = datetime(
        local_date.year,
        local_date.month,
        local_date.day,
        sunset_time[0],
        sunset_time[1],
        tzinfo=tz,
    )
    return {
        'forecast': {
            'sunrise': sunrise_local.isoformat(),
            'sunset': sunset_local.isoformat(),
            'periods': [],
        }
    }


def build_measurements(local_dt, lux_value, points=12):
    """Create a short set of synthetic measurements anchored to a local datetime."""
    end_utc = local_dt.astimezone(timezone.utc)
    readings = [
        create_test_measurement(lux_value, end_utc - timedelta(minutes=15 * i))
        for i in range(points)
    ]
    # Return oldest -> newest to mirror production ordering.
    return list(reversed(readings))

def test_scenarios():
    """Test sunrise/sunset-aware lux scenarios."""
    print("Testing Daylight-Aware Lux Anomaly Detection")
    print("=" * 80)

    tz = ZoneInfo('America/New_York')
    local_time = datetime.now(tz)
    print(f"\nReference local date: {local_time.strftime('%Y-%m-%d')} ({local_time.strftime('%Z')})")

    nws_context = make_nws_context_for_date(local_time.date(), tz)
    sunrise = nws_context['forecast']['sunrise']
    sunset = nws_context['forecast']['sunset']
    print(f"Sunrise: {sunrise}")
    print(f"Sunset: {sunset}")
    print("\n" + "-" * 80)

    # Test 1: Pre-sunrise dark should not be anomalous.
    print("\n### Test 1: Pre-sunrise darkness (expected) ###")
    pre_sunrise_dt = datetime(local_time.year, local_time.month, local_time.day, 6, 10, tzinfo=tz)
    measurements = build_measurements(pre_sunrise_dt, lux_value=20.0)
    result = analyze_lux_anomaly(measurements, nws_data=nws_context)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Time period: {result.get('time_period')}")
    print(f"Source: {result.get('classification_source')}")
    print(f"Reason: {result.get('reason', 'None - this is expected pre-sunrise darkness')}")
    assert result['anomalous'] is False
    assert result.get('classification_source') == 'nws_solar'

    # Test 2: Shortly after sunrise with very low light should be anomalous.
    print("\n### Test 2: Post-sunrise darkness (anomalous) ###")
    post_sunrise_dt = datetime(local_time.year, local_time.month, local_time.day, 8, 10, tzinfo=tz)
    measurements = build_measurements(post_sunrise_dt, lux_value=80.0)
    result = analyze_lux_anomaly(measurements, nws_data=nws_context)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Time period: {result.get('time_period')}")
    print(f"Reason: {result.get('reason', 'None')}")
    assert result['anomalous'] is True
    assert result.get('time_period') == 'daylight'

    # Test 3: Late summer style bright evening before sunset should not be anomalous.
    print("\n### Test 3: Bright evening before sunset (expected) ###")
    pre_sunset_dt = datetime(local_time.year, local_time.month, local_time.day, 19, 0, tzinfo=tz)
    measurements = build_measurements(pre_sunset_dt, lux_value=4000.0)
    result = analyze_lux_anomaly(measurements, nws_data=nws_context)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Time period: {result.get('time_period')}")
    print(f"Reason: {result.get('reason', 'None - this is expected before sunset')}")
    assert result['anomalous'] is False
    assert result.get('time_period') == 'daylight'

    # Test 4: Bright late night should be anomalous.
    print("\n### Test 4: Bright night after twilight (anomalous) ###")
    late_night_dt = datetime(local_time.year, local_time.month, local_time.day, 23, 30, tzinfo=tz)
    measurements = build_measurements(late_night_dt, lux_value=1200.0)
    result = analyze_lux_anomaly(measurements, nws_data=nws_context)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Time period: {result.get('time_period')}")
    print(f"Reason: {result.get('reason', 'None')}")
    assert result['anomalous'] is True
    assert result.get('time_period') == 'night'

    # Test 5: Missing solar context should gracefully fallback to legacy logic.
    print("\n### Test 5: Fallback classification when sunrise/sunset missing ###")
    fallback_dt = datetime(local_time.year, local_time.month, local_time.day, 9, 0, tzinfo=tz)
    measurements = build_measurements(fallback_dt, lux_value=120.0)
    result = analyze_lux_anomaly(measurements, nws_data={"forecast": {"periods": []}})
    print(f"Anomalous: {result['anomalous']}")
    print(f"Time period: {result.get('time_period')}")
    print(f"Source: {result.get('classification_source')}")
    print(f"Fallback reason: {result.get('fallback_reason')}")
    assert result.get('classification_source') == 'hour_fallback'
    assert result.get('fallback_reason') == 'missing_sunrise_sunset'

    # Test 6: Previous-day solar data should be accepted via adjacent-day adjustment.
    print("\n### Test 6: Adjacent-day sunrise/sunset reuse ###")
    yesterday_context = make_nws_context_for_date(local_time.date() - timedelta(days=1), tz)
    adjusted_dt = datetime(local_time.year, local_time.month, local_time.day, 8, 30, tzinfo=tz)
    measurements = build_measurements(adjusted_dt, lux_value=500.0)
    result = analyze_lux_anomaly(measurements, nws_data=yesterday_context)
    print(f"Source: {result.get('classification_source')}")
    print(f"Daylight quality: {result.get('daylight_context', {}).get('daylight_data_quality')}")
    print(f"Source day offset: {result.get('daylight_context', {}).get('source_day_offset_days')}")
    assert result.get('classification_source') == 'nws_solar_adjusted'
    assert result.get('daylight_context', {}).get('daylight_data_quality') == 'adjacent_day_adjusted'
    assert result.get('daylight_context', {}).get('source_day_offset_days') == -1
    
    print("\n" + "=" * 80)
    print("\nKey Thresholds:")
    print("  - Daylight: Flags when avg lux < 500 (with stronger messaging below 100)")
    print("  - Night: Flags when avg lux > 500")
    print("  - Twilight: Flags only extreme values")
    print("  - Twilight window: 45 minutes before sunrise and after sunset")
    print("  - Adjacent-day solar data is reused when offset is within 1 day")
    print("\nResult: Light anomalies are aware of seasonal daylight and DST shifts.")

if __name__ == "__main__":
    test_scenarios()
