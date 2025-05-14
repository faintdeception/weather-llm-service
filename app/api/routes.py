"""
API routes for the Weather LLM microservice
"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging

from ..models.prediction import (
    PredictionRequest, 
    PredictionResponse, 
    PredictionResult,
    ScheduleInfo
)
from ..services.llm_service import generate_weather_prediction, get_daily_report

# Configure logging
logger = logging.getLogger("llm-service.api")

# Create API router
router = APIRouter(prefix="/api/predictions", tags=["predictions"])

# Dependency to get database connection
def get_db():
    """Get MongoDB database connection"""
    from ..database.connection import get_database, close_connection
    import os
    
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
        force=request.force
    )
    
    return PredictionResponse(
        success=True,
        message="Prediction generation started in the background",
        prediction=None
    )

@router.get("/latest", response_model=PredictionResponse)
async def get_latest_prediction(db = Depends(get_db)):
    """Get the latest weather prediction"""
    try:
        # Get yesterday's date by default
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
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
        now = datetime.now()
        next_run_datetime = datetime(now.year, now.month, now.day, 6, 0, 0)
        
        if now.hour >= 6:
            # If it's already past 6 AM, next run is tomorrow
            next_run_datetime += timedelta(days=1)
            
        schedule_info = ScheduleInfo(
            next_prediction=next_run_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            schedule_frequency="Daily at 6:00 AM",
            last_prediction=latest_prediction.get('created_at') if latest_prediction else None
        )
        
        return schedule_info
    except Exception as e:
        logger.exception(f"Error retrieving schedule information: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve schedule info: {str(e)}"
        )