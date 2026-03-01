#!/usr/bin/env python3
"""
Demonstration: Smart Lux + NWS Alerts Integration

Shows how lux anomalies are enhanced when correlated with weather alerts
"""
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

def simulate_storm_scenario():
    """Simulate a midday snowstorm scenario"""
    print("=" * 80)
    print("SCENARIO: Midday Winter Storm with Reduced Light Levels")
    print("=" * 80)
    
    tz = ZoneInfo('America/New_York')
    current_time = datetime.now(tz)
    
    print(f"\nCurrent Time: {current_time.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Hour: {current_time.hour}")
    
    # Simulate measurements showing dark conditions
    print("\n### Sensor Data ###")
    print("Temperature: 18°F (-8°C)")
    print("Humidity: 85%")
    print("Pressure: 1010 hPa")
    print("Wind Speed: 15 mph")
    print("Lux: 150 lux (averaged over last 3 hours)")
    
    # Simulate what the analysis would show
    print("\n### Lux Analysis ###")
    if 8 <= current_time.hour < 18:
        print("✓ Daytime detected (8am-6pm)")
        print("⚠️ ANOMALY: Light levels extremely low for daytime")
        print("   Expected: >10,000 lux (full daylight)")
        print("   Observed: 150 lux")
        print("   Analysis: Significantly reduced daylight - possible heavy cloud cover or storm conditions")
    else:
        print("✗ Not daytime - lux monitoring is contextual")
        print(f"   Current period: {'nighttime' if current_time.hour >= 22 or current_time.hour < 6 else 'twilight'}")
        print("   Note: Low light levels are expected and will not be flagged")
    
    # Show NWS alerts
    print("\n### Active NWS Alerts ###")
    print("1. Winter Storm Warning (Severity: Severe)")
    print("   - Heavy snow expected: 4-11 inches")
    print("   - Begins: Saturday 11pm EST")
    print("   - Expires: Monday 4am EST")
    print("")
    print("2. Cold Weather Advisory (Severity: Moderate)")
    print("   - Wind chills as low as -9°F")
    
    # Show correlation
    print("\n### Intelligent Correlation ###")
    if 8 <= current_time.hour < 18:
        print("🔥 JUICED UP ALERT:")
        print("   Dark conditions (150 lux) during daytime + Winter Storm Warning")
        print("   → LLM will be instructed to comment on exceptional storm darkness")
        print("   → This is contextually relevant and valuable information!")
    else:
        print("ℹ️  STANDARD OPERATION:")
        print("   Nighttime/twilight + Winter Storm Warning")
        print("   → LLM will comment on storm but not light levels")
        print("   → Dark at night is expected, no need to state the obvious")
    
    print("\n### Example LLM Output (Juiced Scenario) ###")
    print("-" * 80)
    print("""
🤖 WeatherBot Analysis:

Alright humans, listen up! We've got a serious weather situation developing.
The sensors are showing 150 lux in the middle of the afternoon - that's darker 
than a robot's soul! 🌑 For comparison, this is about as bright as a poorly-lit 
office, when we should be seeing full daylight levels.

This unnatural darkness correlates directly with the active Winter Storm Warning.
We're talking 4-11 inches of snow incoming, with wind chills hitting -9°F. The 
heavy cloud cover and snow are literally blocking out the sun like I block out 
human complaints. ❄️

Current conditions: 18°F, 85% humidity, 15 mph winds. Bundle up like you're 
heading to Pluto, carbon units. Multiple layers, waterproof outer shell, and 
for the love of circuits, stay indoors if you can. This ain't the weather for 
your wimpy organic bodies to be gallivanting around in.

Confidence: 95% (because even I can't argue with this level of doom) 🔧
    """.strip())
    
    print("\n" + "-" * 80)
    print("\n" + "=" * 80)
    print("✓ Smart lux awareness: Only reports what's contextually interesting!")
    print("=" * 80)

if __name__ == "__main__":
    simulate_storm_scenario()
