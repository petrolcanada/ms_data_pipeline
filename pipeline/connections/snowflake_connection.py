"""
Snowflake Connection Manager
Manages Snowflake database connections with SSO support
"""
import snowflake.connector
from typing import Optional
from pipeline.connections.base_connection import BaseConnectionManager
from pipeline.config.settings import get_settings, get_snowflake_connection_params
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeConnectionManager(BaseConnectionManager):
    """
    Manages Snowflake database connections
    
    Features:
    - Single SSO authentication per session
    - Connection reuse across multiple operations
    - Automatic reconnection on connection loss
    - Context manager support for automatic cleanup
    
    Usage:
        # Single operation
        with SnowflakeConnectionManager() as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        
        # Multiple operations (reuses connection)
        with SnowflakeConnectionManager() as conn_mgr:
            extractor = SnowflakeDataExtractor(conn_mgr)
            extractor.estimate_table_size(...)
            extractor.extract_table_chunks(...)  # Reuses same connection
    """
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self._connection_params = None
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish connection to Snowflake
        
        Returns:
            Snowflake connection object
        """
        try:
            if self._connection_params is None:
                self._connection_params = get_snowflake_connection_params()
            
            logger.info(f"Connecting to Snowflake: {self.settings.snowflake_account}")
            
            # Check if using SSO
            if self._connection_params.get('authenticator') == 'externalbrowser':
                logger.info("Using SSO authentication - browser window will open")
            
            connection = snowflake.connector.connect(**self._connection_params)
            
            logger.info("Successfully connected to Snowflake")
            
            # Log connection details
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                warehouse, database, schema = cursor.fetchone()
                logger.info(f"Connected to: {warehouse}.{database}.{schema}")
            finally:
                cursor.close()
            
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection is not None and self._is_connected:
            try:
                self._connection.close()
                logger.info("Closed Snowflake connection")
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")
            finally:
                self._connection = None
                self._is_connected = False
    
    def is_alive(self) -> bool:
        """
        Check if Snowflake connection is alive
        
        Returns:
            True if connection is active, False otherwise
        """
        if self._connection is None:
            return False
        
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            logger.debug("Connection is not alive")
            return False
    
    def execute_query(self, query: str, params: Optional[dict] = None):
        """
        Execute a query using the managed connection
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Cursor with query results
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            cursor.close()
            logger.error(f"Query execution failed: {e}")
            raise
