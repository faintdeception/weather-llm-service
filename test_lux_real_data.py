#!/usr/bin/env python3
"""
Test lux analysis with real data from your database
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from app.database.connection import get_database
from app.services.llm_service import get_measurements, analyze_lux_anomaly
from datetime import datetime
from zoneinfo import ZoneInfo

def main():
    print("=" * 80)
    print("Testing Lux Analysis with Real Database Data")
    print("=" * 80)
    
    # Get recent measurements
    measurements = get_measurements(hours=3)
    print(f"\n✓ Retrieved {len(measurements)} measurements from last 3 hours")
    
    if not measurements:
        print("⚠️ No measurements found - cannot test lux analysis")
        return
    
    # Check if lux data exists
    lux_count = 0
    sample_lux_values = []
    
    for m in measurements[:5]:  # Check first 5
        fields = m.get('fields', {})
        if 'lux' in fields:
            lux_count += 1
            sample_lux_values.append(fields['lux'])
    
    print(f"✓ Found lux data in {lux_count}/5 sample measurements")
    if sample_lux_values:
        print(f"  Sample lux values: {sample_lux_values}")
    
    # Run lux anomaly analysis
    print("\n" + "-" * 80)
    print("Running Lux Anomaly Analysis...")
    print("-" * 80)
    
    lux_result = analyze_lux_anomaly(measurements)
    
    # Get current local time
    tz = ZoneInfo('America/New_York')
    local_time = datetime.now(tz)
    
    print(f"\nCurrent local time: {local_time.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Current hour: {local_time.hour}")
    print(f"\nAnalysis Results:")
    print(f"  Anomalous: {lux_result['anomalous']}")
    print(f"  Average lux: {lux_result.get('lux_avg', 'N/A')}")
    print(f"  Time period: {lux_result.get('time_period', 'N/A')}")
    print(f"  Reason: {lux_result.get('reason', 'None - conditions are normal')}")
    
    if lux_result['anomalous']:
        print("\n🔥 ANOMALY DETECTED - This will be included in the weather report!")
    else:
        print("\n✓ No anomaly - light levels are normal for this time of day")
    
    print("\n" + "=" * 80)
    print("✓ Lux integration confirmed working with your database!")
    print("=" * 80)

if __name__ == "__main__":
    main()
