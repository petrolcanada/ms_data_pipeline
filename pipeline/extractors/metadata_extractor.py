"""
Snowflake Metadata Extractor
Extracts table schemas and metadata from Snowflake and saves to local repository
"""
import json
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import snowflake.connector
from pipeline.config.settings import get_settings, get_snowflake_connection_params
from pipeline.utils.logger import get_logger
from pipeline.utils.metadata_comparator import MetadataComparator
from pipeline.utils.change_logger import ChangeLogger

logger = get_logger(__name__)

class SnowflakeMetadataExtractor:
    def __init__(self, obfuscator=None):
        self.settings = get_settings()
        self.metadata_dir = Path("metadata/schemas")
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.ddl_dir = Path("metadata/ddl")
        self.ddl_dir.mkdir(parents=True, exist_ok=True)
        self.comparator = MetadataComparator()
        self.change_logger = ChangeLogger(obfuscator=obfuscator)
        self.obfuscator = obfuscator  # Optional MetadataObfuscator instance
        
    def connect_to_snowflake(self):
        """
        Establish connection to Snowflake (VPN side) with SSO support
        Uses connection parameters matching successful YAML practice
        """
        try:
            # Get connection parameters based on settings
            conn_params = get_snowflake_connection_params()
            
            logger.info(f"Connecting to Snowflake account: {self.settings.snowflake_account}")
            logger.info(f"Using authentication method: {self.settings.snowflake_auth_method}")
            
            if self.settings.snowflake_auth_method == "sso":
                logger.info("SSO authentication will open a browser window. Please complete authentication in your browser.")
            
            # Connect with parameters matching successful YAML practice
            conn = snowflake.connector.connect(**conn_params)
            
            logger.info(f"✅ Successfully connected to Snowflake")
            logger.info(f"   User: {self.settings.snowflake_user}")
            logger.info(f"   Role: {self.settings.snowflake_role}")
            logger.info(f"   Warehouse: {self.settings.snowflake_warehouse}")
            logger.info(f"   Database: {self.settings.snowflake_database}")
            logger.info(f"   Schema: {self.settings.snowflake_schema}")
            
            return conn
            
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Snowflake database error: {e}")
            if "250001" in str(e):
                logger.error("Could not connect to Snowflake backend. Check account identifier and VPN connection.")
            elif "authentication" in str(e).lower():
                logger.error("Authentication failed. Verify credentials and complete SSO in browser if prompted.")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            if "externalbrowser" in str(e).lower() or "browser" in str(e).lower():
                logger.info("SSO authentication requires a browser. Ensure you're running in an environment with browser access.")
            raise
    
    def extract_table_metadata(self, database: str, schema: str, table: str, conn=None) -> Dict[str, Any]:
        """
        Extract complete metadata for a specific table
        
        Args:
            database: Snowflake database name
            schema: Snowflake schema name
            table: Snowflake table name
            conn: Optional existing Snowflake connection (if None, creates new connection)
            
        Returns:
            Dictionary with table metadata
        """
        # Use provided connection or create new one
        should_close = False
        if conn is None:
            conn = self.connect_to_snowflake()
            should_close = True
        
        cursor = conn.cursor()
        
        try:
            logger.info(f"Extracting metadata for {database}.{schema}.{table}")
            
            # Get table schema information
            schema_query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                ORDINAL_POSITION
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(schema_query)
            columns = cursor.fetchall()
            
            if not columns:
                raise ValueError(f"Table {database}.{schema}.{table} not found or has no columns")
            
            # Get table statistics
            stats_query = f"""
            SELECT 
                ROW_COUNT,
                BYTES,
                LAST_ALTERED
            FROM {database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{table}'
            """
            
            cursor.execute(stats_query)
            stats = cursor.fetchone()
            
            # Get primary key information
            # Skipping primary key extraction for now
            primary_keys = []
            logger.debug(f"Primary key extraction skipped for {database}.{schema}.{table}")
            
            # Build metadata structure
            metadata = {
                "table_info": {
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "full_name": f"{database}.{schema}.{table}"
                },
                "columns": [],
                "statistics": {
                    "row_count": stats[0] if stats else 0,
                    "size_bytes": stats[1] if stats else 0,
                    "last_altered": str(stats[2]) if stats else None
                },
                "primary_keys": primary_keys,
                "extracted_at": str(pd.Timestamp.now())
            }
            
            # Process columns
            for col in columns:
                column_info = {
                    "name": col[0],
                    "data_type": col[1],
                    "is_nullable": col[2] == 'YES',
                    "default_value": col[3],
                    "max_length": col[4],
                    "precision": col[5],
                    "scale": col[6],
                    "position": col[7],
                    "postgres_type": self._map_to_postgres_type(col[1], col[4], col[5], col[6])
                }
                metadata["columns"].append(column_info)
            
            logger.info(f"✅ Extracted metadata for {database}.{schema}.{table}")
            logger.info(f"   Columns: {len(metadata['columns'])}")
            logger.info(f"   Rows: {metadata['statistics']['row_count']:,}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to extract metadata for {table}: {e}")
            raise
        finally:
            cursor.close()
            # Only close connection if we created it
            if should_close:
                conn.close()
    
    def _map_to_postgres_type(self, snowflake_type: str, max_length: int, precision: int, scale: int) -> str:
        """Map Snowflake data types to PostgreSQL equivalents"""
        type_mapping = {
            'NUMBER': self._map_number_type(precision, scale),
            'DECIMAL': self._map_number_type(precision, scale),
            'NUMERIC': self._map_number_type(precision, scale),
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'BYTEINT': 'SMALLINT',
            'FLOAT': 'DOUBLE PRECISION',
            'FLOAT4': 'REAL',
            'FLOAT8': 'DOUBLE PRECISION',
            'DOUBLE': 'DOUBLE PRECISION',
            'DOUBLE PRECISION': 'DOUBLE PRECISION',
            'REAL': 'REAL',
            'VARCHAR': f'VARCHAR({max_length})' if max_length else 'TEXT',
            'CHAR': f'CHAR({max_length})' if max_length else 'CHAR(1)',
            'CHARACTER': f'CHAR({max_length})' if max_length else 'CHAR(1)',
            'STRING': 'TEXT',
            'TEXT': 'TEXT',
            'BINARY': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'TIMESTAMP_LTZ': 'TIMESTAMP WITH TIME ZONE',
            'TIMESTAMP_NTZ': 'TIMESTAMP',
            'TIMESTAMP_TZ': 'TIMESTAMP WITH TIME ZONE',
            'VARIANT': 'JSONB',
            'OBJECT': 'JSONB',
            'ARRAY': 'JSONB'
        }
        
        return type_mapping.get(snowflake_type.upper(), 'TEXT')
    
    def _map_number_type(self, precision: int, scale: int) -> str:
        """Map Snowflake NUMBER type to appropriate PostgreSQL type"""
        if scale == 0:  # Integer
            if precision <= 4:
                return 'SMALLINT'
            elif precision <= 9:
                return 'INTEGER'
            elif precision <= 18:
                return 'BIGINT'
            else:
                return 'NUMERIC'
        else:  # Decimal
            if precision and scale:
                return f'NUMERIC({precision},{scale})'
            else:
                return 'NUMERIC'
    
    def check_metadata_changed(self, table_name: str, new_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Check if metadata has changed compared to existing file
        
        Args:
            table_name: Name of the table
            new_metadata: Newly extracted metadata
            
        Returns:
            Comparison result dict if existing metadata found, None if first extraction
        """
        metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
        
        if not metadata_file.exists():
            logger.info(f"No existing metadata found for {table_name} - this is the first extraction")
            return None
        
        try:
            with open(metadata_file, 'r') as f:
                old_metadata = json.load(f)
            
            comparison = self.comparator.compare_metadata(old_metadata, new_metadata)
            
            if comparison["has_changes"]:
                logger.warning(f"⚠️  Metadata changes detected for {table_name}")
                logger.warning(f"   {comparison['summary']}")
            else:
                logger.info(f"No metadata changes detected for {table_name}")
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing metadata for {table_name}: {e}")
            return None
    
    def check_metadata_changed_obfuscated(
        self,
        table_name: str,
        new_metadata: Dict[str, Any],
        password: str
    ) -> Optional[Dict[str, Any]]:
        """
        Check for changes when obfuscation is enabled
        
        Decrypts the previous metadata file IN MEMORY (not to disk),
        compares with new metadata, then returns comparison results.
        
        Args:
            table_name: Name of the table
            new_metadata: Newly extracted metadata
            password: Decryption password
            
        Returns:
            Comparison result dict or None if first extraction
        """
        # Get file ID for this table
        file_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata")
        encrypted_file = self.metadata_dir / f"{file_id}.enc"
        
        if not encrypted_file.exists():
            logger.info(f"No existing metadata found for {table_name} - this is the first extraction")
            return None
        
        try:
            # Decrypt to MEMORY (not disk)
            decrypted_bytes = self.obfuscator.encryptor.decrypt_to_memory(encrypted_file, password)
            
            # Parse JSON from memory
            old_metadata = json.loads(decrypted_bytes.decode('utf-8'))
            
            # Compare metadata (all in memory)
            comparison = self.comparator.compare_metadata(old_metadata, new_metadata)
            
            if comparison["has_changes"]:
                logger.warning(f"⚠️  Metadata changes detected for {table_name}")
                logger.warning(f"   {comparison['summary']}")
            else:
                logger.info(f"No metadata changes detected for {table_name}")
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing obfuscated metadata for {table_name}: {e}")
            return None
    
    def archive_old_metadata(self, table_name: str) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Archive existing metadata and DDL files with timestamp
        
        Args:
            table_name: Name of the table
            
        Returns:
            Tuple of (archived_metadata_path, archived_ddl_path)
        """
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Archive metadata file
        metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
        archived_metadata = None
        if metadata_file.exists():
            archived_metadata = self.metadata_dir / f"{table_name}_{date_str}_metadata.json"
            metadata_file.rename(archived_metadata)
            logger.info(f"Archived metadata to {archived_metadata}")
        
        # Archive DDL file
        ddl_file = self.ddl_dir / f"{table_name}_create.sql"
        archived_ddl = None
        if ddl_file.exists():
            archived_ddl = self.ddl_dir / f"{table_name}_{date_str}_create.sql"
            ddl_file.rename(archived_ddl)
            logger.info(f"Archived DDL to {archived_ddl}")
        
        return archived_metadata, archived_ddl
    
    def archive_old_metadata_obfuscated(
        self,
        table_name: str,
        password: str
    ) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Archive obfuscated metadata files with timestamped filenames
        
        Process:
        1. Decrypt existing encrypted files to memory
        2. Create timestamped raw filenames (table_YYYYMMDD_metadata.json)
        3. Encrypt with new deterministic IDs based on timestamped names
        4. Delete old encrypted files
        
        Args:
            table_name: Name of the table
            password: Encryption password
            
        Returns:
            Tuple of (archived_metadata_path, archived_ddl_path)
        """
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Get current file IDs (without timestamp)
        current_metadata_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata")
        current_ddl_id = self.obfuscator.generate_metadata_file_id(table_name, "ddl")
        
        # Get archived file IDs (with timestamp)
        archived_metadata_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata", date_str)
        archived_ddl_id = self.obfuscator.generate_metadata_file_id(table_name, "ddl", date_str)
        
        archived_metadata = None
        archived_ddl = None
        
        # Archive metadata file
        current_metadata_file = self.metadata_dir / f"{current_metadata_id}.enc"
        if current_metadata_file.exists():
            try:
                # Decrypt to memory
                decrypted_bytes = self.obfuscator.encryptor.decrypt_to_memory(current_metadata_file, password)
                
                # Encrypt with new timestamped ID
                archived_metadata = self.metadata_dir / f"{archived_metadata_id}.enc"
                self.obfuscator.encryptor.encrypt_from_memory(decrypted_bytes, archived_metadata, password)
                
                # Delete old encrypted file
                current_metadata_file.unlink()
                
                logger.info(f"Archived metadata: {current_metadata_id}.enc → {archived_metadata_id}.enc")
                logger.info(f"  (Represents: {table_name}_metadata.json → {table_name}_metadata_{date_str}.json)")
                
            except Exception as e:
                logger.error(f"Failed to archive metadata for {table_name}: {e}")
                archived_metadata = None
        
        # Archive DDL file
        current_ddl_file = self.ddl_dir / f"{current_ddl_id}.enc"
        if current_ddl_file.exists():
            try:
                # Decrypt to memory
                decrypted_bytes = self.obfuscator.encryptor.decrypt_to_memory(current_ddl_file, password)
                
                # Encrypt with new timestamped ID
                archived_ddl = self.ddl_dir / f"{archived_ddl_id}.enc"
                self.obfuscator.encryptor.encrypt_from_memory(decrypted_bytes, archived_ddl, password)
                
                # Delete old encrypted file
                current_ddl_file.unlink()
                
                logger.info(f"Archived DDL: {current_ddl_id}.enc → {archived_ddl_id}.enc")
                logger.info(f"  (Represents: {table_name}_create.sql → {table_name}_create_{date_str}.sql)")
                
            except Exception as e:
                logger.error(f"Failed to archive DDL for {table_name}: {e}")
                archived_ddl = None
        
        return archived_metadata, archived_ddl
    
    def save_metadata_to_file(self, metadata: Dict[str, Any], table_name: str, check_changes: bool = False, password: Optional[str] = None) -> Tuple[Path, Optional[Dict[str, Any]]]:
        """
        Save metadata to local JSON file in the repository
        
        Args:
            metadata: Metadata to save
            table_name: Name of the table
            check_changes: Whether to check for changes before saving
            password: Encryption password (required if obfuscation enabled)
            
        Returns:
            Tuple of (metadata_file_path, comparison_result)
        """
        comparison = None
        archived_files = None
        
        if check_changes:
            # Check for changes (obfuscated or non-obfuscated)
            if self.obfuscator and password:
                comparison = self.check_metadata_changed_obfuscated(table_name, metadata, password)
            else:
                comparison = self.check_metadata_changed(table_name, metadata)
            
            if comparison is None:
                # First extraction - log it
                created_files = None
                if self.obfuscator:
                    metadata_file_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata")
                    ddl_file_id = self.obfuscator.generate_metadata_file_id(table_name, "ddl")
                    created_files = {
                        'metadata': self.metadata_dir / f"{metadata_file_id}.enc",
                        'ddl': self.ddl_dir / f"{ddl_file_id}.enc"
                    }
                else:
                    created_files = {
                        'metadata': self.metadata_dir / f"{table_name}_metadata.json",
                        'ddl': self.ddl_dir / f"{table_name}_create.sql"
                    }
                self.change_logger.log_initial_extraction(table_name, created_files, password)
                
            elif comparison.get("changed"):
                # Archive old files before saving new ones
                if self.obfuscator:
                    archived_metadata, archived_ddl = self.archive_old_metadata_obfuscated(table_name, password)
                else:
                    archived_metadata, archived_ddl = self.archive_old_metadata(table_name)
                
                archived_files = {
                    'metadata': archived_metadata,
                    'ddl': archived_ddl
                }
                
                # Log the changes with archived file paths
                self.change_logger.log_change(
                    table_name, 
                    comparison["changes"], 
                    comparison["summary"],
                    archived_files,
                    password
                )
        
        # Determine file path based on obfuscation
        if self.obfuscator:
            # Obfuscated: generate deterministic file ID
            if not password:
                raise ValueError("Password required for obfuscated metadata")
            
            file_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata")
            metadata_file = self.metadata_dir / f"{file_id}.enc"
            
            # Save as temporary JSON file
            temp_json = self.metadata_dir / f"{file_id}.json.tmp"
            with open(temp_json, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            # Encrypt the file
            logger.info(f"Encrypting metadata for {table_name}...")
            self.obfuscator.encryptor.encrypt_file(temp_json, metadata_file, password)
            
            # Remove temporary file
            temp_json.unlink()
            
            logger.info(f"Saved encrypted metadata to {metadata_file}")
        else:
            # Non-obfuscated: use table name
            metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            logger.info(f"Saved metadata to {metadata_file}")
        
        return metadata_file, comparison
    
    def generate_postgres_ddl(self, metadata: Dict[str, Any], postgres_schema: str, postgres_table: str) -> str:
        """Generate PostgreSQL CREATE TABLE DDL from metadata"""
        ddl_lines = [f"CREATE TABLE IF NOT EXISTS {postgres_schema}.{postgres_table} ("]
        
        column_definitions = []
        
        # Add insertion timestamp column as the first column
        column_definitions.append("    data_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL")
        
        # Add all original columns from Snowflake
        for col in metadata["columns"]:
            col_def = f"    {col['name']} {col['postgres_type']}"
            if not col['is_nullable']:
                col_def += " NOT NULL"
            if col['default_value']:
                col_def += f" DEFAULT {col['default_value']}"
            column_definitions.append(col_def)
        
        ddl_lines.append(",\n".join(column_definitions))
        
        # Add primary key constraint if exists
        if metadata["primary_keys"]:
            pk_cols = ", ".join(metadata["primary_keys"])
            ddl_lines.append(f",\n    PRIMARY KEY ({pk_cols})")
        
        ddl_lines.append(");")
        
        # Add comments
        ddl_lines.append(f"\n-- Source: {metadata['table_info']['full_name']}")
        ddl_lines.append(f"-- Extracted: {metadata['extracted_at']}")
        ddl_lines.append(f"-- Rows: {metadata['statistics']['row_count']}")
        ddl_lines.append(f"-- Note: data_inserted_at column tracks when data was inserted into PostgreSQL")
        
        return "\n".join(ddl_lines)
    
    def save_postgres_ddl(self, ddl: str, table_name: str, password: Optional[str] = None) -> Path:
        """
        Save PostgreSQL DDL to file
        
        Args:
            ddl: DDL string to save
            table_name: Name of the table
            password: Encryption password (required if obfuscation enabled)
            
        Returns:
            Path to saved DDL file
        """
        # Determine file path based on obfuscation
        if self.obfuscator:
            # Obfuscated: generate random file ID
            if not password:
                raise ValueError("Password required for obfuscated DDL")
            
            file_id = self.obfuscator.generate_metadata_file_id(table_name, "ddl")
            ddl_file = self.ddl_dir / f"{file_id}.enc"
            
            # Save as temporary SQL file
            temp_sql = self.ddl_dir / f"{file_id}.sql.tmp"
            with open(temp_sql, 'w') as f:
                f.write(ddl)
            
            # Encrypt the file
            logger.info(f"Encrypting DDL for {table_name}...")
            self.obfuscator.encryptor.encrypt_file(temp_sql, ddl_file, password)
            
            # Remove temporary file
            temp_sql.unlink()
            
            logger.info(f"Saved encrypted DDL to {ddl_file}")
        else:
            # Non-obfuscated: use table name
            ddl_file = self.ddl_dir / f"{table_name}_create.sql"
            with open(ddl_file, 'w') as f:
                f.write(ddl)
            
            logger.info(f"Saved DDL to {ddl_file}")
        
        return ddl_file
    
    def extract_all_configured_tables(self, check_changes: bool = False, force: bool = False, password: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract metadata for all tables in config/tables.yaml
        
        Args:
            check_changes: Whether to check for metadata changes
            force: Force re-extraction even if no changes detected
            password: Encryption password (required if obfuscation enabled)
            
        Returns:
            Dictionary with extraction results for each table
        """
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        results = {}
        
        # Create single Snowflake connection for all tables
        logger.info("Establishing Snowflake connection for all tables...")
        conn = self.connect_to_snowflake()
        
        try:
            for table_config in config["tables"]:
                table_name = table_config["name"]
                sf_config = table_config["snowflake"]
                pg_config = table_config["postgres"]
                
                try:
                    logger.info(f"Processing table: {table_name}")
                    
                    # Extract metadata from Snowflake (reuse connection)
                    metadata = self.extract_table_metadata(
                        sf_config["database"],
                        sf_config["schema"],
                        sf_config["table"],
                        conn=conn  # Pass existing connection
                    )
                    
                    # Save metadata to file (with optional change checking)
                    metadata_file, comparison = self.save_metadata_to_file(
                        metadata, 
                        table_name, 
                        check_changes=check_changes,
                        password=password
                    )
                    
                    # Determine if we should skip DDL generation
                    skip_ddl = False
                    if check_changes and comparison and not comparison["has_changes"] and not force:
                        logger.info(f"Skipping DDL generation for {table_name} (no changes detected)")
                        skip_ddl = True
                    
                    # Generate PostgreSQL DDL
                    ddl = self.generate_postgres_ddl(
                        metadata,
                        pg_config["schema"],
                        pg_config["table"]
                    )
                    
                    # Save DDL to file
                    ddl_file = self.save_postgres_ddl(ddl, table_name, password=password)
                    
                    results[table_name] = {
                        "status": "success",
                        "metadata_file": str(metadata_file),
                        "ddl_file": str(ddl_file),
                        "columns": len(metadata["columns"]),
                        "row_count": metadata["statistics"]["row_count"],
                        "has_changes": comparison["has_changes"] if comparison else None,
                        "is_new": comparison is None,
                        "comparison": comparison
                    }
                    
                except Exception as e:
                    logger.error(f"Failed to process table {table_name}: {e}")
                    results[table_name] = {
                        "status": "error",
                        "error": str(e)
                    }
        finally:
            # Close the shared connection
            logger.info("Closing Snowflake connection")
            conn.close()
        
        return results

if __name__ == "__main__":
    import pandas as pd
    extractor = SnowflakeMetadataExtractor()
    results = extractor.extract_all_configured_tables()
    print("Metadata extraction results:")
    for table, result in results.items():
        print(f"  {table}: {result['status']}")