#!/usr/bin/env python3
"""
Test script to reproduce the exact datetime storage issue
"""
import os
import sys
from datetime import datetime, timezone

# Set up environment variables for testing
os.environ.setdefault('MONGO_URI', 'mongodb://localhost:27017')
os.environ.setdefault('MONGO_DB', 'weather_test_db')

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

def test_exact_llm_service_flow():
    """Replicate the exact flow from llm_service.py"""
    print("Testing exact LLM service flow...")
    
    try:
        from app.database.connection import get_database
        
        # Get database (same as llm_service)
        db = get_database()
        
        # Create prediction document exactly like llm_service does
        current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Step 1: Create the initial document (exactly like llm_service.py line 232-236)
        prediction_doc = {
            "date": current_date,
            "location": "test_location",
            "created_at": datetime.now(timezone.utc),
        }
        
        print(f"1. Initial prediction_doc:")
        print(f"   created_at: {prediction_doc['created_at']} (type: {type(prediction_doc['created_at'])})")
        
        # Step 2: Simulate the prediction result processing
        prediction_result = {
            'prediction_12h': {'temp': '20C', 'condition': 'sunny'},
            'prediction_24h': {'temp': '18C', 'condition': 'cloudy'},
            'reasoning': 'Test reasoning',
            'confidence': 0.85
        }
        
        # Step 3: Update the document (exactly like llm_service.py lines 247-253)
        prediction_doc.update({
            "prediction_12h": prediction_result.get('prediction_12h', {}),
            "prediction_24h": prediction_result.get('prediction_24h', {}),
            "reasoning": prediction_result.get('reasoning', ""),
            "confidence": prediction_result.get('confidence', 0.0)
        })
        
        print(f"2. After update():")
        print(f"   created_at: {prediction_doc['created_at']} (type: {type(prediction_doc['created_at'])})")
        
        # Step 4: Insert exactly like llm_service.py
        print(f"3. About to insert with created_at: {prediction_doc['created_at']}")
        result = db['test_weather_predictions'].insert_one(prediction_doc)
        print(f"4. Inserted with ID: {result.inserted_id}")
        
        # Step 5: Retrieve and check
        stored_doc = db['test_weather_predictions'].find_one({"_id": result.inserted_id})
        print(f"5. Retrieved created_at: {stored_doc['created_at']} (type: {type(stored_doc['created_at'])})")
        
        # Check if it's stored as string
        if isinstance(stored_doc['created_at'], str):
            print(f"   ❌ PROBLEM: DateTime was stored as string!")
            print(f"   String value: '{stored_doc['created_at']}'")
        else:
            print(f"   ✅ OK: DateTime was stored correctly as {type(stored_doc['created_at'])}")
        
        # Clean up
        db['test_weather_predictions'].delete_one({"_id": result.inserted_id})
        print("6. Test document cleaned up")
        
    except Exception as e:
        print(f"❌ Error in test: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_exact_llm_service_flow()
