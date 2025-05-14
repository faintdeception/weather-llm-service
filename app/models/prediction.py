"""
Prediction data models for the Weather LLM microservice
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

class PredictionRequest(BaseModel):
    """Request model for generating a weather prediction"""
    date: Optional[str] = None
    force: bool = False
    location: Optional[str] = None

class PredictionResult(BaseModel):
    """Model for weather prediction results"""
    date: str
    location: str
    created_at: str
    prediction_12h: Dict[str, Any]
    prediction_24h: Dict[str, Any]
    reasoning: str
    confidence: float

class PredictionResponse(BaseModel):
    """Response model for prediction API endpoints"""
    success: bool
    message: str
    prediction: Optional[PredictionResult] = None

class ScheduleInfo(BaseModel):
    """Information about the prediction schedule"""
    next_prediction: str
    schedule_frequency: str
    last_prediction: Optional[str] = None