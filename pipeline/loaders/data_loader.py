"""
PostgreSQL Data Loader
Loads data from Parquet files to PostgreSQL tables
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from pipeline.config.settings import get_settings, get_postgres_connection_params
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLDataLoader:
    """Load data from Parquet files to PostgreSQL tables"""
    
    def __init__(self):
        self.settings = get_settings()
        
    def connect_to_postgres(self):
        """Establish connection to PostgreSQL"""
        try:
            conn_params = get_postgres_connection_params()
            conn = psycopg2.connect(**conn_params)
            logger.info(f"Connected to PostgreSQL: {self.settings.postgres_host}:{self.settings.postgres_port}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def load_parquet_to_table(
        self, 
        parquet_path: Path, 
        schema: str, 
        table: str,
        batch_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Load Parquet file to PostgreSQL table
        
        Args:
            parquet_path: Path to Parquet file
            schema: PostgreSQL schema name
            table: PostgreSQL table name
            batch_size: Number of rows per batch insert
            
        Returns:
            Dictionary with load statistics
        """
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        
        try:
            # Read Parquet file
            logger.info(f"Reading {parquet_path.name}")
            df = pd.read_parquet(parquet_path, engine='pyarrow')
            
            total_rows = len(df)
            logger.info(f"  Rows: {total_rows:,}")
            
            # Get column names
            columns = df.columns.tolist()
            
            # Prepare INSERT statement
            placeholders = ','.join(['%s'] * len(columns))
            columns_str = ','.join([f'"{col}"' for col in columns])
            insert_sql = f'INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})'
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.values]
            
            # Batch insert
            logger.info(f"Loading to {schema}.{table}")
            execute_batch(cursor, insert_sql, data, page_size=batch_size)
            conn.commit()
            
            logger.info(f"✅ Loaded {total_rows:,} rows to {schema}.{table}")
            
            return {
                "rows_loaded": total_rows,
                "table": f"{schema}.{table}",
                "status": "success"
            }
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load data: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def verify_row_count(self, schema: str, table: str, expected_count: int) -> bool:
        """
        Verify row count in PostgreSQL table
        
        Args:
            schema: PostgreSQL schema name
            table: PostgreSQL table name
            expected_count: Expected number of rows
            
        Returns:
            True if count matches
        """
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {schema}.{table}')
            actual_count = cursor.fetchone()[0]
            
            matches = actual_count == expected_count
            
            if matches:
                logger.info(f"✅ Row count verified: {actual_count:,} rows")
            else:
                logger.error(f"❌ Row count mismatch!")
                logger.error(f"  Expected: {expected_count:,}")
                logger.error(f"  Actual: {actual_count:,}")
            
            return matches
            
        finally:
            cursor.close()
            conn.close()
    
    def truncate_table(self, schema: str, table: str):
        """
        Truncate PostgreSQL table
        
        Args:
            schema: PostgreSQL schema name
            table: PostgreSQL table name
        """
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f'TRUNCATE TABLE {schema}.{table}')
            conn.commit()
            logger.info(f"Truncated {schema}.{table}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to truncate table: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        """
        Get table information
        
        Args:
            schema: PostgreSQL schema name
            table: PostgreSQL table name
            
        Returns:
            Dictionary with table info
        """
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        
        try:
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM {schema}.{table}')
            row_count = cursor.fetchone()[0]
            
            # Get table size
            cursor.execute(f"""
                SELECT pg_size_pretty(pg_total_relation_size('{schema}.{table}'))
            """)
            table_size = cursor.fetchone()[0]
            
            logger.info(f"Table {schema}.{table}:")
            logger.info(f"  Rows: {row_count:,}")
            logger.info(f"  Size: {table_size}")
            
            return {
                "row_count": row_count,
                "table_size": table_size
            }
            
        finally:
            cursor.close()
            conn.close()
