#!/usr/bin/env python3
"""
Test script for smart lux anomaly detection
"""
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Simulate measurements with different lux scenarios
def create_test_measurement(lux_value, hours_ago=0):
    """Create a test measurement with specific lux value"""
    timestamp = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return {
        'timestamp_ms': timestamp,
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

def test_scenarios():
    """Test various lux scenarios"""
    print("Testing Smart Lux Anomaly Detection")
    print("=" * 80)
    
    # Get current local time for context
    tz = ZoneInfo('America/New_York')
    local_time = datetime.now(tz)
    hour = local_time.hour
    
    print(f"\nCurrent local time: {local_time.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Current hour: {hour}")
    print("\n" + "-" * 80)
    
    # Test 1: Normal nighttime darkness (should NOT flag)
    print("\n### Test 1: Normal nighttime darkness ###")
    measurements = [create_test_measurement(5.0, i/4) for i in range(12)]  # 5 lux over 3 hours
    result = analyze_lux_anomaly(measurements)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Average lux: {result.get('lux_avg', 'N/A')}")
    print(f"Reason: {result.get('reason', 'None - this is normal')}")
    
    # Test 2: Dark during daytime (should flag if it's daytime)
    print("\n### Test 2: Unusually dark during daytime ###")
    measurements = [create_test_measurement(200.0, i/4) for i in range(12)]  # 200 lux 
    result = analyze_lux_anomaly(measurements)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Average lux: {result.get('lux_avg', 'N/A')}")
    print(f"Time period: {result.get('time_period', 'N/A')}")
    print(f"Reason: {result.get('reason', 'None')}")
    
    # Test 3: Extremely dark during daytime - storm conditions (should definitely flag)
    print("\n### Test 3: Storm-level darkness during daytime ###")
    measurements = [create_test_measurement(50.0, i/4) for i in range(12)]  # 50 lux
    result = analyze_lux_anomaly(measurements)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Average lux: {result.get('lux_avg', 'N/A')}")
    print(f"Time period: {result.get('time_period', 'N/A')}")
    print(f"Reason: {result.get('reason', 'None')}")
    
    # Test 4: Normal bright daytime (should NOT flag)
    print("\n### Test 4: Normal bright daytime ###")
    measurements = [create_test_measurement(8000.0, i/4) for i in range(12)]  # 8000 lux
    result = analyze_lux_anomaly(measurements)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Average lux: {result.get('lux_avg', 'N/A')}")
    print(f"Time period: {result.get('time_period', 'N/A')}")
    print(f"Reason: {result.get('reason', 'None - this is normal')}")
    
    # Test 5: Bright at night (should flag)
    print("\n### Test 5: Unusually bright at nighttime ###")
    measurements = [create_test_measurement(1000.0, i/4) for i in range(12)]  # 1000 lux at night
    result = analyze_lux_anomaly(measurements)
    print(f"Anomalous: {result['anomalous']}")
    print(f"Average lux: {result.get('lux_avg', 'N/A')}")
    print(f"Time period: {result.get('time_period', 'N/A')}")
    print(f"Reason: {result.get('reason', 'None')}")
    
    print("\n" + "=" * 80)
    print("\nKey Thresholds:")
    print("  - Daytime (8am-6pm): Flags if < 500 lux (storm/heavy clouds)")
    print("  - Nighttime (10pm-6am): Flags if > 500 lux (unusual brightness)")
    print("  - Twilight (6-8am, 6-10pm): Flags only extreme cases")
    print("\nResult: Only contextually interesting anomalies will be reported!")

if __name__ == "__main__":
    test_scenarios()
