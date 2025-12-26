"""
Logging Configuration
Provides structured logging for the application
"""
import logging
import sys
from typing import Optional
from pipeline.config.settings import get_settings

def setup_logging():
    """Setup application logging configuration"""
    settings = get_settings()
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )
    
    # Set specific logger levels
    logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Get logger instance for a module"""
    return logging.getLogger(name)