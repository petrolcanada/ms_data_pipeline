"""
Base Connection Manager
Provides abstract base class for connection management
"""
from abc import ABC, abstractmethod
from typing import Any, Optional
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class BaseConnectionManager(ABC):
    """Abstract base class for connection management"""
    
    def __init__(self):
        self._connection: Optional[Any] = None
        self._is_connected: bool = False
    
    @abstractmethod
    def connect(self) -> Any:
        """
        Establish connection to database
        
        Returns:
            Database connection object
        """
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    def is_alive(self) -> bool:
        """
        Check if connection is alive
        
        Returns:
            True if connection is active, False otherwise
        """
        pass
    
    def get_connection(self) -> Any:
        """
        Get active connection, creating one if necessary
        
        Returns:
            Database connection object
        """
        if self._connection is None or not self.is_alive():
            logger.info("Creating new connection")
            self._connection = self.connect()
            self._is_connected = True
        else:
            logger.debug("Reusing existing connection")
        
        return self._connection
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection is closed"""
        self.close()
        return False  # Don't suppress exceptions
    
    def __del__(self):
        """Destructor - cleanup connection if not already closed"""
        if self._is_connected:
            try:
                self.close()
            except Exception as e:
                logger.warning(f"Error closing connection in destructor: {e}")
