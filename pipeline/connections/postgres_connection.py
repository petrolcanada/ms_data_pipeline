"""
PostgreSQL Connection Manager
Manages PostgreSQL database connections
"""
import psycopg2
from typing import Optional
from pipeline.connections.base_connection import BaseConnectionManager
from pipeline.config.settings import get_settings, get_postgres_connection_params
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class PostgresConnectionManager(BaseConnectionManager):
    """
    Manages PostgreSQL database connections
    
    Features:
    - Connection reuse across multiple operations
    - Transaction management support
    - Automatic reconnection on connection loss
    - Context manager support for automatic cleanup
    
    Usage:
        # Single operation
        with PostgresConnectionManager() as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        
        # Multiple operations (reuses connection)
        with PostgresConnectionManager() as conn_mgr:
            loader = PostgreSQLDataLoader(conn_mgr)
            loader.truncate_table(...)
            loader.load_parquet_to_table(...)  # Reuses same connection
    """
    
    def __init__(self, autocommit: bool = False):
        """
        Initialize PostgreSQL connection manager
        
        Args:
            autocommit: If True, enable autocommit mode
        """
        super().__init__()
        self.settings = get_settings()
        self._connection_params = None
        self.autocommit = autocommit
    
    def connect(self) -> psycopg2.extensions.connection:
        """
        Establish connection to PostgreSQL
        
        Returns:
            PostgreSQL connection object
        """
        try:
            if self._connection_params is None:
                self._connection_params = get_postgres_connection_params()
            
            logger.info(f"Connecting to PostgreSQL: {self.settings.postgres_host}:{self.settings.postgres_port}")
            
            connection = psycopg2.connect(**self._connection_params)
            
            # Set autocommit if requested
            if self.autocommit:
                connection.autocommit = True
                logger.debug("Autocommit enabled")
            
            logger.info("Successfully connected to PostgreSQL")
            
            # Log connection details
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT current_database(), current_user, version()")
                database, user, version = cursor.fetchone()
                logger.info(f"Connected to: {database} as {user}")
                logger.debug(f"PostgreSQL version: {version}")
            finally:
                cursor.close()
            
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def close(self):
        """Close PostgreSQL connection"""
        if self._connection is not None and self._is_connected:
            try:
                # Rollback any pending transactions
                if not self.autocommit and not self._connection.closed:
                    try:
                        self._connection.rollback()
                    except Exception:
                        pass
                
                self._connection.close()
                logger.info("Closed PostgreSQL connection")
            except Exception as e:
                logger.warning(f"Error closing PostgreSQL connection: {e}")
            finally:
                self._connection = None
                self._is_connected = False
    
    def is_alive(self) -> bool:
        """
        Check if PostgreSQL connection is alive
        
        Returns:
            True if connection is active, False otherwise
        """
        if self._connection is None:
            return False
        
        try:
            if self._connection.closed:
                return False
            
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            logger.debug("Connection is not alive")
            return False
    
    def commit(self):
        """Commit current transaction"""
        if self._connection is not None and not self.autocommit:
            try:
                self._connection.commit()
                logger.debug("Transaction committed")
            except Exception as e:
                logger.error(f"Failed to commit transaction: {e}")
                raise
    
    def rollback(self):
        """Rollback current transaction"""
        if self._connection is not None and not self.autocommit:
            try:
                self._connection.rollback()
                logger.debug("Transaction rolled back")
            except Exception as e:
                logger.error(f"Failed to rollback transaction: {e}")
                raise
    
    def execute_query(self, query: str, params: Optional[tuple] = None, commit: bool = False):
        """
        Execute a query using the managed connection
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            commit: If True, commit after execution
            
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
            
            if commit and not self.autocommit:
                self.commit()
            
            return cursor
        except Exception as e:
            if not self.autocommit:
                self.rollback()
            cursor.close()
            logger.error(f"Query execution failed: {e}")
            raise
