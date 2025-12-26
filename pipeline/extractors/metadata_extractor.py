"""
Snowflake Metadata Extractor
Extracts table schemas and metadata from Snowflake and saves to local repository
"""
import json
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import snowflake.connector
from pipeline.config.settings import get_settings, get_snowflake_connection_params
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)

class SnowflakeMetadataExtractor:
    def __init__(self):
        self.settings = get_settings()
        self.metadata_dir = Path("metadata/schemas")
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
    def connect_to_snowflake(self):
        """Establish connection to Snowflake (VPN side) with SSO support"""
        try:
            # Get connection parameters based on auth method
            conn_params = get_snowflake_connection_params()
            
            conn = snowflake.connector.connect(**conn_params)
            logger.info(f"Successfully connected to Snowflake using {self.settings.snowflake_auth_method} authentication")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            if "externalbrowser" in str(e).lower():
                logger.info("SSO authentication requires opening a browser window. Please complete the authentication in your browser.")
            raise
    
    def extract_table_metadata(self, database: str, schema: str, table: str) -> Dict[str, Any]:
        """Extract complete metadata for a specific table"""
        conn = self.connect_to_snowflake()
        cursor = conn.cursor()
        
        try:
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
            pk_query = f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = '{schema}' 
            AND tc.TABLE_NAME = '{table}'
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """
            
            cursor.execute(pk_query)
            primary_keys = [row[0] for row in cursor.fetchall()]
            
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
            
            logger.info(f"Extracted metadata for {database}.{schema}.{table}")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to extract metadata for {table}: {e}")
            raise
        finally:
            cursor.close()
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
    
    def save_metadata_to_file(self, metadata: Dict[str, Any], table_name: str):
        """Save metadata to local JSON file in the repository"""
        metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Saved metadata to {metadata_file}")
        return metadata_file
    
    def generate_postgres_ddl(self, metadata: Dict[str, Any], postgres_schema: str, postgres_table: str) -> str:
        """Generate PostgreSQL CREATE TABLE DDL from metadata"""
        ddl_lines = [f"CREATE TABLE IF NOT EXISTS {postgres_schema}.{postgres_table} ("]
        
        column_definitions = []
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
        
        return "\n".join(ddl_lines)
    
    def save_postgres_ddl(self, ddl: str, table_name: str):
        """Save PostgreSQL DDL to file"""
        ddl_dir = Path("metadata/ddl")
        ddl_dir.mkdir(parents=True, exist_ok=True)
        
        ddl_file = ddl_dir / f"{table_name}_create.sql"
        with open(ddl_file, 'w') as f:
            f.write(ddl)
        
        logger.info(f"Saved DDL to {ddl_file}")
        return ddl_file
    
    def extract_all_configured_tables(self) -> Dict[str, Any]:
        """Extract metadata for all tables in config/tables.yaml"""
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        results = {}
        
        for table_config in config["tables"]:
            table_name = table_config["name"]
            sf_config = table_config["snowflake"]
            pg_config = table_config["postgres"]
            
            try:
                # Extract metadata from Snowflake
                metadata = self.extract_table_metadata(
                    sf_config["database"],
                    sf_config["schema"],
                    sf_config["table"]
                )
                
                # Save metadata to file
                metadata_file = self.save_metadata_to_file(metadata, table_name)
                
                # Generate PostgreSQL DDL
                ddl = self.generate_postgres_ddl(
                    metadata,
                    pg_config["schema"],
                    pg_config["table"]
                )
                
                # Save DDL to file
                ddl_file = self.save_postgres_ddl(ddl, table_name)
                
                results[table_name] = {
                    "status": "success",
                    "metadata_file": str(metadata_file),
                    "ddl_file": str(ddl_file),
                    "columns": len(metadata["columns"]),
                    "row_count": metadata["statistics"]["row_count"]
                }
                
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}")
                results[table_name] = {
                    "status": "error",
                    "error": str(e)
                }
        
        return results

if __name__ == "__main__":
    import pandas as pd
    extractor = SnowflakeMetadataExtractor()
    results = extractor.extract_all_configured_tables()
    print("Metadata extraction results:")
    for table, result in results.items():
        print(f"  {table}: {result['status']}")