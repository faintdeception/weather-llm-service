#!/usr/bin/env python3
"""
Test script for NWS integration
"""
import sys
import json
from app.services.nws_service import (
    get_nws_data,
    format_alerts_for_prompt,
    format_forecast_for_prompt
)

def main():
    print("Fetching NWS data for Arlington, VA (38.7692672, -77.0890822)...")
    print("=" * 80)
    
    nws_data = get_nws_data()
    
    print("\n### ALERTS ###")
    print(format_alerts_for_prompt(nws_data['alerts']))
    
    print("\n### FORECAST ###")
    print(format_forecast_for_prompt(nws_data['forecast']))
    
    print("\n### RAW DATA SUMMARY ###")
    print(f"Fetched at: {nws_data['fetched_at']}")
    print(f"Number of alerts: {len(nws_data['alerts'])}")
    print(f"Has forecast: {bool(nws_data['forecast'])}")
    
    if nws_data['alerts']:
        print("\nAlert details:")
        for alert in nws_data['alerts']:
            print(f"  - {alert['event']}")
            print(f"    Severity: {alert['severity']}, Urgency: {alert['urgency']}, Certainty: {alert['certainty']}")
            print(f"    Area: {alert['areaDesc']}")
            if alert['headline']:
                print(f"    Headline: {alert['headline'][:100]}...")
    
    print("\n" + "=" * 80)
    print("NWS Integration Test Complete!")

if __name__ == "__main__":
    main()
