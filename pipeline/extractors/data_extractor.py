"""
Snowflake Data Extractor
Extracts data from Snowflake tables in chunks and saves as compressed Parquet files
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Any, Generator, Optional, List
import snowflake.connector
from pipeline.config.settings import get_settings
from pipeline.connections import SnowflakeConnectionManager
from pipeline.transformers.type_optimizer import optimize_dataframe
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeDataExtractor:
    """Extract data from Snowflake tables in manageable chunks"""
    
    def __init__(self, connection_manager: Optional[SnowflakeConnectionManager] = None):
        """
        Initialize data extractor
        
        Args:
            connection_manager: Optional SnowflakeConnectionManager instance.
                              If not provided, creates a new one (not recommended for multiple operations)
        """
        self.settings = get_settings()
        self._conn_manager = connection_manager
        self._owns_connection = connection_manager is None
        
        # Create connection manager if not provided
        if self._conn_manager is None:
            logger.warning("No connection manager provided - creating new one. Consider passing a connection manager for better performance.")
            self._conn_manager = SnowflakeConnectionManager()
    
    def get_connection(self):
        """Get Snowflake connection from manager"""
        return self._conn_manager.get_connection()
    
    def close(self):
        """Close connection if we own it"""
        if self._owns_connection and self._conn_manager is not None:
            self._conn_manager.close()
    
    def _build_filter_clause(self, filter_config) -> str:
        """
        Build SQL filter clause from configuration
        
        Handles WHERE conditions and QUALIFY clauses correctly.
        QUALIFY is a Snowflake-specific clause that must come after WHERE,
        not be joined with AND.
        
        Args:
            filter_config: String or list of filter conditions
            
        Returns:
            Complete WHERE/QUALIFY clause or empty string
        """
        if not filter_config:
            return ""
        
        # Handle single string filter
        if isinstance(filter_config, str):
            filter_str = filter_config.strip()
            if not filter_str:
                return ""
            # If it already starts with WHERE or QUALIFY, use as-is
            if filter_str.upper().startswith(("WHERE", "QUALIFY")):
                return filter_str
            # Otherwise, prepend WHERE
            return f"WHERE {filter_str}"
        
        # Handle list of filters
        if isinstance(filter_config, list):
            # Filter out empty strings
            filters = [f.strip() for f in filter_config if f and f.strip()]
            
            if not filters:
                return ""
            
            # Separate WHERE conditions from QUALIFY clauses
            where_conditions = []
            qualify_clauses = []
            
            for filter_item in filters:
                filter_upper = filter_item.upper()
                if filter_upper.startswith("QUALIFY"):
                    # This is a QUALIFY clause
                    qualify_clauses.append(filter_item)
                else:
                    # This is a WHERE condition
                    where_conditions.append(filter_item)
            
            # Build WHERE clause
            result = ""
            if where_conditions:
                first_condition = where_conditions[0]
                if first_condition.upper().startswith("WHERE"):
                    result = first_condition
                else:
                    result = f"WHERE {first_condition}"
                
                # Add remaining WHERE conditions
                for condition in where_conditions[1:]:
                    # If it starts with AND/OR, use as-is
                    if condition.upper().startswith(("AND", "OR")):
                        result += f" {condition}"
                    else:
                        # Otherwise, prepend AND
                        result += f" AND {condition}"
            
            # Add QUALIFY clauses (they come after WHERE, not joined with AND)
            for qualify_clause in qualify_clauses:
                if result:
                    result += f" {qualify_clause}"
                else:
                    result = qualify_clause
            
            return result
        
        logger.warning(f"Invalid filter configuration type: {type(filter_config)}")
        return ""
    
    def estimate_table_size(
        self, 
        database: str, 
        schema: str, 
        table: str,
        filter_clause: str = None,
        base_query: str = None,
    ) -> Dict[str, Any]:
        """
        Estimate table size and row count
        
        Args:
            database: Snowflake database name
            schema: Snowflake schema name
            table: Snowflake table name
            filter_clause: Optional WHERE clause to filter rows
        
        Returns:
            Dictionary with row_count, size_bytes, size_mb
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # If filter is provided, count filtered rows
            if filter_clause:
                select_from = base_query if base_query else f"SELECT * FROM {database}.{schema}.{table}"
                count_query = f"""
                SELECT COUNT(*) 
                FROM (
                    {select_from}
                    {filter_clause}
                ) AS filtered_data
                """
                
                logger.debug(f"Estimating filtered table size...")
                logger.debug(f"  Count query: {count_query.strip()}")
                
                # Now execute
                cursor.execute(count_query)
                row_count = cursor.fetchone()[0] or 0
                
                # Estimate size based on filtered row count
                # Get average row size from table metadata
                metadata_query = f"""
                SELECT BYTES, ROW_COUNT
                FROM {database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table}'
                """
                cursor.execute(metadata_query)
                result = cursor.fetchone()
                
                if result and result[1] and result[1] > 0:
                    total_bytes = result[0] or 0
                    total_rows = result[1]
                    avg_row_size = total_bytes / total_rows
                    estimated_bytes = int(avg_row_size * row_count)
                else:
                    estimated_bytes = 0
                
                size_mb = estimated_bytes / (1024 * 1024)
                
                logger.info(f"Filtered table {database}.{schema}.{table}:")
                logger.info(f"  Filtered rows: {row_count:,}")
                logger.info(f"  Estimated size: {size_mb:.2f} MB")
                
                return {
                    "row_count": row_count,
                    "size_bytes": estimated_bytes,
                    "size_mb": size_mb,
                    "filtered": True
                }
            else:
                # No filter - use table metadata
                query = f"""
                SELECT 
                    ROW_COUNT,
                    BYTES
                FROM {database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table}'
                """
                
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result:
                    row_count = result[0] or 0
                    size_bytes = result[1] or 0
                    size_mb = size_bytes / (1024 * 1024)
                    
                    logger.info(f"Table {database}.{schema}.{table}:")
                    logger.info(f"  Rows: {row_count:,}")
                    logger.info(f"  Size: {size_mb:.2f} MB")
                    
                    return {
                        "row_count": row_count,
                        "size_bytes": size_bytes,
                        "size_mb": size_mb,
                        "filtered": False
                    }
                else:
                    raise ValueError(f"Table {database}.{schema}.{table} not found")
        
        except Exception as e:
            # Log the error with context
            logger.error(f"Failed to estimate table size: {e}")
            if filter_clause:
                logger.error(f"  Failed query was: SELECT COUNT(*) FROM {database}.{schema}.{table} {filter_clause}")
                print(f"\n❌ Query execution failed!")
                print(f"   Error: {e}")
            raise
                
        finally:
            cursor.close()
    
    def inject_watermark(
        self,
        filter_clause: str,
        watermark_column: str,
        watermark_value: str,
    ) -> str:
        """Append a watermark predicate to an existing filter clause.

        If there is already a WHERE clause the watermark is ANDed in;
        otherwise a new WHERE is created. QUALIFY clauses are preserved
        after the watermark.
        """
        predicate = f"{watermark_column} > '{watermark_value}'"

        if not filter_clause:
            return f"WHERE {predicate}"

        upper = filter_clause.upper()
        has_where = "WHERE" in upper
        qualify_idx = upper.find("QUALIFY")

        if qualify_idx == -1:
            joiner = "AND" if has_where else "WHERE"
            return f"{filter_clause} {joiner} {predicate}"

        before_qualify = filter_clause[:qualify_idx].rstrip()
        qualify_part = filter_clause[qualify_idx:]
        has_where_before = "WHERE" in before_qualify.upper()
        joiner = "AND" if has_where_before else "WHERE"
        return f"{before_qualify} {joiner} {predicate} {qualify_part}"

    def extract_table_chunks(
        self, 
        database: str, 
        schema: str, 
        table: str, 
        chunk_size: int = 100000,
        order_by: str = None,
        filter_clause: str = None,
        base_query: str = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Extract table data in chunks
        
        Args:
            database: Snowflake database name
            schema: Snowflake schema name
            table: Snowflake table name
            chunk_size: Number of rows per chunk
            order_by: Column to order by (for consistent chunking)
            filter_clause: Optional WHERE clause to filter rows
            
        Yields:
            DataFrame chunks
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Build query
            query = base_query if base_query else f"SELECT * FROM {database}.{schema}.{table}"
            
            # Add filter clause if provided
            if filter_clause:
                query += f" {filter_clause}"
            
            # Add order by if provided
            if order_by:
                query += f" ORDER BY {order_by}"
            
            # Log and print query BEFORE execution
            logger.info(f"Extracting data from {database}.{schema}.{table}")
            if filter_clause:
                logger.debug(f"  Filter: {filter_clause}")
            logger.debug(f"  Chunk size: {chunk_size:,} rows")
            logger.debug(f"  Full query: {query}")
            
            # Execute query
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            chunk_num = 0
            total_rows = 0
            
            while True:
                # Fetch chunk
                rows = cursor.fetchmany(chunk_size)
                
                if not rows:
                    break
                
                chunk_num += 1
                total_rows += len(rows)
                
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=columns)
                
                logger.info(f"  Chunk {chunk_num}: {len(df):,} rows (total: {total_rows:,})")
                
                yield df
            
            logger.info(f"Extraction complete: {total_rows:,} total rows in {chunk_num} chunks")
            
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            logger.error(f"Query was: {query}")
            print(f"\n❌ Data extraction query failed!")
            print(f"   Error: {e}")
            print(f"   Query was: {query}")
            raise
        finally:
            cursor.close()
    
    def _detect_dictionary_columns(self, df: pd.DataFrame, threshold: float = 0.5) -> List[str]:
        """
        Identify columns that benefit from Parquet dictionary encoding.
        
        Low-cardinality columns (unique ratio below threshold) compress
        dramatically better with dictionary encoding.
        """
        dict_cols = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype == 'object' or dtype.name == 'category':
                n_unique = df[col].nunique()
                n_total = len(df)
                if n_total > 0 and (n_unique / n_total) < threshold:
                    dict_cols.append(col)
            elif dtype.name in ('int8', 'int16', 'bool'):
                dict_cols.append(col)
        return dict_cols

    def save_chunk_to_parquet(
        self, 
        df: pd.DataFrame, 
        output_path: Path, 
        compression: str = 'zstd',
        compression_level: int = 9,
        optimize_types: bool = True,
        sort_columns: Optional[List[str]] = None,
        use_dictionary_encoding: bool = True,
    ) -> Dict[str, Any]:
        """
        Save DataFrame chunk as compressed Parquet file with advanced encoding.
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            compression: Compression algorithm (snappy, gzip, zstd, brotli)
            compression_level: Compression level (1-9 for gzip, 1-22 for zstd, 0-11 for brotli)
            optimize_types: If True, optimize data types before compression
            sort_columns: Columns to sort by before writing (improves compression ratio)
            use_dictionary_encoding: If True, auto-detect and apply dictionary encoding
            
        Returns:
            Dictionary with file metadata
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            optimization_stats = None
            if optimize_types:
                logger.info("Optimizing data types for better compression...")
                df, optimization_stats = optimize_dataframe(df, aggressive=False)
            
            # Pre-sort: sorting by clustered columns improves RLE and dictionary
            # encoding within Parquet row groups
            sorted_by = []
            if sort_columns:
                valid_sort = [c for c in sort_columns if c in df.columns]
                if valid_sort:
                    df = df.sort_values(by=valid_sort, ignore_index=True)
                    sorted_by = valid_sort
                    logger.info(f"Pre-sorted by {valid_sort} for better compression")
            
            # Convert to PyArrow table for fine-grained encoding control
            table = pa.Table.from_pandas(df, preserve_index=False)
            
            # Auto-detect low-cardinality columns for dictionary encoding
            dict_columns = []
            if use_dictionary_encoding:
                dict_columns = self._detect_dictionary_columns(df)
                if dict_columns:
                    logger.debug(f"Dictionary encoding for {len(dict_columns)} columns: {dict_columns[:5]}...")
            
            # Build per-column compression spec — use byte_stream_split for
            # float columns (better for real-valued data) when the main codec is zstd
            column_compression = None
            if compression in ('zstd', 'gzip', 'brotli'):
                column_compression = {}
                for field in table.schema:
                    col_name = field.name
                    if pa.types.is_floating(field.type):
                        column_compression[col_name] = {
                            'codec': compression,
                            'encoding': 'BYTE_STREAM_SPLIT',
                        }
            
            # Write with PyArrow for encoding control
            pq.write_table(
                table,
                str(output_path),
                compression=compression,
                compression_level=compression_level if compression in ('gzip', 'zstd', 'brotli') else None,
                use_dictionary=dict_columns if dict_columns else False,
                write_statistics=True,
                data_page_size=1024 * 1024,
            )
            
            file_size = output_path.stat().st_size
            
            logger.info(f"Saved {output_path.name}")
            logger.debug(f"  Rows: {len(df):,}")
            logger.debug(f"  Size: {file_size / (1024 * 1024):.2f} MB")
            logger.debug(f"  Compression: {compression} (level {compression_level})")
            if sorted_by:
                logger.debug(f"  Sorted by: {sorted_by}")
            if dict_columns:
                logger.debug(f"  Dictionary columns: {len(dict_columns)}")
            
            result = {
                "rows": len(df),
                "size_bytes": file_size,
                "size_mb": file_size / (1024 * 1024),
                "compression": compression,
                "compression_level": compression_level,
                "sorted_by": sorted_by,
                "dictionary_columns": len(dict_columns),
            }
            
            if optimization_stats:
                result["type_optimization"] = optimization_stats
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to save Parquet file: {e}")
            raise
