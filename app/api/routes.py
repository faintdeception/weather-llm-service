"""
API routes for the Weather LLM microservice
"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from datetime import datetime, timedelta, timezone
import logging

from ..models.prediction import (
    PredictionRequest, 
    PredictionResponse, 
    ScheduleInfo
)
from ..services.llm_service import generate_weather_prediction

# Configure logging
logger = logging.getLogger("llm-service.api")

# Create API router
router = APIRouter(prefix="/api/predictions", tags=["predictions"])


def _jsonify(value):
    """Convert Mongo-native values into JSON-safe primitives."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)

# Dependency to get database connection
def get_db():
    """Get MongoDB database connection"""
    from ..database.connection import get_database
    
    # Get database using the proper connection manager
    db = get_database()
    
    try:
        yield db
    finally:
        # Don't close the connection here as it's managed globally
        # The close_connection() function can be called at application shutdown
        pass

@router.post("/request", response_model=PredictionResponse)
async def request_prediction(
    request: PredictionRequest,
    background_tasks: BackgroundTasks,
    db = Depends(get_db)
):
    """
    Request a weather prediction to be generated asynchronously
    
    The prediction will be generated in the background and can be retrieved
    later using the /latest endpoint.
    """
    logger.info(f"Received prediction request: {request}")
    
    # Add the prediction task to background tasks
    background_tasks.add_task(
        generate_weather_prediction, 
        db, 
        date=request.date,
        force_cache_overwrite=request.force,
        hours_to_analyze=None  # None lets the service pick the env default
    )
    
    return PredictionResponse(
        success=True,
        message="Prediction generation started in the background",
        prediction=None
    )

@router.get("/latest", response_model=PredictionResponse)
async def get_latest_prediction(db = Depends(get_db)):
    """Get the latest weather prediction"""
    try:        # Get yesterday's date by default
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Find the latest prediction
        prediction = db['weather_predictions'].find_one(
            {'date': yesterday},
            sort=[('created_at', -1)]
        )
        
        if not prediction:
            return PredictionResponse(
                success=False,
                message="No prediction found for yesterday",
                prediction=None
            )
        
        # Remove MongoDB's _id field
        if '_id' in prediction:
            del prediction['_id']
            
        return PredictionResponse(
            success=True,
            message="Latest prediction retrieved successfully",
            prediction=prediction
        )
    except Exception as e:
        logger.exception(f"Error retrieving latest prediction: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve latest prediction: {str(e)}"
        )

@router.get("/by-date/{date}", response_model=PredictionResponse)
async def get_prediction_by_date(date: str, db = Depends(get_db)):
    """Get a weather prediction for a specific date"""
    try:
        # Find the prediction for the specified date
        prediction = db['weather_predictions'].find_one({'date': date})
        
        if not prediction:
            return PredictionResponse(
                success=False,
                message=f"No prediction found for date: {date}",
                prediction=None
            )
        
        # Remove MongoDB's _id field
        if '_id' in prediction:
            del prediction['_id']
            
        return PredictionResponse(
            success=True,
            message=f"Prediction for {date} retrieved successfully",
            prediction=prediction
        )
    except Exception as e:
        logger.exception(f"Error retrieving prediction for date {date}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve prediction: {str(e)}"
        )

@router.get("/schedule", response_model=ScheduleInfo)
async def get_schedule_info(db = Depends(get_db)):
    """Get information about the prediction schedule"""
    try:
        # Get the most recent prediction
        latest_prediction = db['weather_predictions'].find_one(
            sort=[('created_at', -1)]
        )
          # Calculate next prediction time (predictions run daily at 6 AM)
        now = datetime.now(timezone.utc)
        next_run_datetime = datetime(now.year, now.month, now.day, 6, 0, 0, tzinfo=timezone.utc)
        
        if now.hour >= 6:
            # If it's already past 6 AM, next run is tomorrow
            next_run_datetime += timedelta(days=1)
            
        schedule_info = ScheduleInfo(
            next_prediction=next_run_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            schedule_frequency="Daily at 6:00 AM",
            last_prediction=latest_prediction.get('created_at').strftime("%Y-%m-%d %H:%M:%S") if latest_prediction and latest_prediction.get('created_at') else None
        )
        
        return schedule_info
    except Exception as e:
        logger.exception(f"Error retrieving schedule information: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve schedule info: {str(e)}"
        )


@router.get("/nws/latest")
async def get_latest_nws_snapshot(db = Depends(get_db)):
    """Get the most recent cached NWS payload."""
    try:
        snapshot = db['nws_snapshots'].find_one(sort=[('created_at', -1)])

        if not snapshot:
            return {
                "success": False,
                "message": "No cached NWS snapshot found",
                "nws_snapshot": None,
            }

        return {
            "success": True,
            "message": "Latest cached NWS snapshot retrieved successfully",
            "nws_snapshot": _jsonify(snapshot),
        }
    except Exception as e:
        logger.exception(f"Error retrieving latest NWS snapshot: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve latest NWS snapshot: {str(e)}"
        )


@router.get("/nws/by-date/{date}")
async def get_nws_snapshot_by_date(date: str, db = Depends(get_db)):
    """Get the latest cached NWS payload associated with a report date (YYYY-MM-DD)."""
    try:
        snapshot = db['nws_snapshots'].find_one(
            {'report_date': date},
            sort=[('created_at', -1)]
        )

        if not snapshot:
            return {
                "success": False,
                "message": f"No cached NWS snapshot found for date: {date}",
                "nws_snapshot": None,
            }

        return {
            "success": True,
            "message": f"Cached NWS snapshot for {date} retrieved successfully",
            "nws_snapshot": _jsonify(snapshot),
        }
    except Exception as e:
        logger.exception(f"Error retrieving NWS snapshot for date {date}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve NWS snapshot: {str(e)}"
        )
