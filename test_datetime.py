#!/usr/bin/env python3
"""
Test script to check datetime storage in MongoDB
"""
import os
import sys
from datetime import datetime

# Add the app directory to the path so we can import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

def test_datetime_storage():
    """Test how datetime objects are stored in MongoDB"""
    print("Testing datetime storage behavior...")
    
    # Test 1: Basic datetime object behavior
    test_datetime = datetime.utcnow()
    print(f"1. Original datetime object: {test_datetime}")
    print(f"   Type: {type(test_datetime)}")
    print(f"   String representation: {str(test_datetime)}")
    print(f"   ISO format: {test_datetime.isoformat()}")
    
    # Test 2: Document construction
    test_doc = {
        "test_field": "test_value",
        "created_at": test_datetime,
        "date": test_datetime.strftime('%Y-%m-%d')
    }
    
    print(f"\n2. Document before any processing:")
    for key, value in test_doc.items():
        print(f"   {key}: {value} (type: {type(value)})")
    
    # Test 3: What happens when we try to simulate the exact same process as in llm_service
    prediction_doc = {
        "date": test_datetime.strftime('%Y-%m-%d'),
        "location": "test_location",
        "created_at": datetime.utcnow(),  # This is exactly what we do in the service
    }
    
    print(f"\n3. Prediction doc (simulating llm_service):")
    for key, value in prediction_doc.items():
        print(f"   {key}: {value} (type: {type(value)})")
    
    # Test 4: Check if update() method affects datetime
    prediction_doc.update({
        "prediction_12h": {"test": "data"},
        "prediction_24h": {"test": "data"},
        "reasoning": "test reasoning",
        "confidence": 0.85
    })
    
    print(f"\n4. After update() method:")
    print(f"   created_at: {prediction_doc['created_at']} (type: {type(prediction_doc['created_at'])})")
    
    # Test 5: Check what str() does to datetime
    datetime_str = str(test_datetime)
    print(f"\n5. What str(datetime) produces: '{datetime_str}'")
    print(f"   Does this match your DB value? Compare to: '2025-06-09 20:30:08'")

if __name__ == "__main__":
    test_datetime_storage()
