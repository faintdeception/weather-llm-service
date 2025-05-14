"""
Main entry point for the Weather LLM microservice
"""
import os
import sys
import logging
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("llm-service")

# Import API router
from .api.routes import router as prediction_router
from .services.llm_service import generate_weather_prediction
from .database.connection import get_database, close_connection

# Global scheduler
scheduler = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for the FastAPI application
    Handles startup and shutdown tasks
    """
    # Load environment variables from .env file if present
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logger.info("Loaded environment variables from .env file")
    except ImportError:
        logger.warning("python-dotenv not installed, skipping .env loading")
    
    # Create and start the scheduler
    global scheduler
    scheduler = configure_scheduler()
    
    # Yield control back to FastAPI
    yield
    
    # Shutdown tasks
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shutdown complete")
    
    # Close database connection
    close_connection()
    logger.info("Database connection closed")

def configure_scheduler():
    """Configure and start the background scheduler for periodic tasks"""
    scheduler = BackgroundScheduler()
    
    try:
        # Schedule daily prediction generation at 6 AM
        def run_daily_prediction():
            try:
                # Get yesterday's date (predictions are for the previous day)
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"Running scheduled prediction for {yesterday}")
                
                # Generate the prediction
                result = generate_weather_prediction(date=yesterday)
                
                if result:
                    logger.info(f"Scheduled prediction completed successfully")
                else:
                    logger.error(f"Scheduled prediction failed")
            except Exception as e:
                logger.exception(f"Error in scheduled prediction job: {e}")
        
        # Add the job to run daily at 6 AM
        scheduler.add_job(run_daily_prediction, 'cron', hour=6, minute=0)
        
        # Start the scheduler
        scheduler.start()
        logger.info("Prediction scheduler started successfully")
        
        return scheduler
    except Exception as e:
        logger.exception(f"Failed to configure scheduler: {e}")
        return None

# Create FastAPI application
app = FastAPI(
    title="Weather LLM Microservice",
    description="API for generating weather predictions using LLM models",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(prediction_router)

@app.get("/")
async def root():
    """Root endpoint that returns service information"""
    return {
        "service": "Weather LLM Microservice",
        "version": "1.0.0",
        "status": "running",
        "scheduler_active": scheduler.running if scheduler else False
    }

if __name__ == "__main__":
    import uvicorn
    # Start the server if running directly
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)