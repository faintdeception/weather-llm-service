"""
MongoDB connection manager for the Weather LLM Service

This module provides functions for managing MongoDB connections,
including handling connection closures and reconnection.
"""
import os
import logging
from pymongo import MongoClient
from pymongo.errors import InvalidOperation

# Configure logging
logger = logging.getLogger("llm-service")

# Global client reference
_mongo_client = None

def get_database():
    """
    Get or create MongoDB connection and return database
    
    Returns:
        MongoDB database object
    """
    global _mongo_client
    
    # Create new connection if needed
    if _mongo_client is None:
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB")
        logger.info(f"Creating new MongoDB connection to {db_name}")
        _mongo_client = MongoClient(mongo_uri)
        return _mongo_client[db_name]
    
    # Test if connection is still alive
    try:
        # Ping the database
        _mongo_client.admin.command('ping')
        return _mongo_client[os.getenv("MONGO_DB")]
    except Exception as e:
        logger.warning(f"MongoDB connection check failed: {str(e)}")
        # Reconnect if connection was closed
        try:
            if _mongo_client:
                _mongo_client.close()
        except Exception as close_error:
            logger.warning(f"Error closing MongoDB connection: {str(close_error)}")
        
        logger.info("Reconnecting to MongoDB")
        _mongo_client = MongoClient(os.getenv("MONGO_URI"))
        return _mongo_client[os.getenv("MONGO_DB")]

def with_db_connection(func):
    """
    Decorator to ensure DB connection is valid
    
    Args:
        func: The function to wrap with connection handling
        
    Returns:
        Wrapped function that handles connection errors
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except InvalidOperation as e:
            if "Cannot use MongoClient after close" in str(e):
                logger.info("MongoDB connection was closed, reconnecting...")
                # Reset the global client
                global _mongo_client
                try:
                    if _mongo_client:
                        _mongo_client.close()
                except Exception as close_error:
                    logger.warning(f"Error closing MongoDB connection: {str(close_error)}")
                
                _mongo_client = None
                # Retry once with fresh connection
                return func(*args, **kwargs)
            raise
    return wrapper

def close_connection():
    """Close the MongoDB connection if it exists"""
    global _mongo_client
    if _mongo_client:
        try:
            _mongo_client.close()
            _mongo_client = None
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.warning(f"Error closing MongoDB connection: {str(e)}")