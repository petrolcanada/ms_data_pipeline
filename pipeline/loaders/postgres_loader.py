"""
PostgreSQL Loader
Creates tables in PostgreSQL based on extracted metadata and loads data
"""
import json
import psycopg2
from pathlib import Path
from typing import Dict, Any, List
from pipeline.config.settings import get_settings, get_postgres_connection_params
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)

class PostgreSQLLoader:
    def __init__(self):
        self.settings = get_settings()
        self.metadata_dir = Path("metadata/schemas")
        self.ddl_dir = Path("metadata/ddl")
        
    def connect_to_postgres(self):
        """Establish connection to PostgreSQL (external, non-VPN)"""
        try:
            conn_params = get_postgres_connection_params()
            conn = psycopg2.connect(**conn_params)
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def load_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Load metadata from saved JSON file"""
        metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
        
        if not metadata_file.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_file}")
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        logger.info(f"Loaded metadata for {table_name}")
        return metadata
    
    def load_ddl_script(self, table_name: str) -> str:
        """Load DDL script from saved SQL file"""
        ddl_file = self.ddl_dir / f"{table_name}_create.sql"
        
        if not ddl_file.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_file}")
        
        with open(ddl_file, 'r') as f:
            ddl = f.read()
        
        logger.info(f"Loaded DDL for {table_name}")
        return ddl
    
    def create_table_from_metadata(self, table_name: str, drop_if_exists: bool = False):
        """Create PostgreSQL table using saved metadata and DDL"""
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        
        try:
            # Load metadata and DDL
            metadata = self.load_table_metadata(table_name)
            ddl = self.load_ddl_script(table_name)
            
            postgres_schema = self._extract_schema_from_ddl(ddl)
            postgres_table = self._extract_table_from_ddl(ddl)
            
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {postgres_schema}")
            
            # Drop table if requested
            if drop_if_exists:
                cursor.execute(f"DROP TABLE IF EXISTS {postgres_schema}.{postgres_table} CASCADE")
                logger.info(f"Dropped existing table {postgres_schema}.{postgres_table}")
            
            # Execute DDL to create table
            cursor.execute(ddl)
            conn.commit()
            
            logger.info(f"Created table {postgres_schema}.{postgres_table}")
            
            # Create indexes if needed
            self._create_indexes(cursor, metadata, postgres_schema, postgres_table)
            conn.commit()
            
            return {
                "status": "success",
                "schema": postgres_schema,
                "table": postgres_table,
                "columns": len(metadata["columns"])
            }
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def _extract_schema_from_ddl(self, ddl: str) -> str:
        """Extract schema name from DDL"""
        import re
        match = re.search(r'CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)', ddl)
        if match:
            return match.group(1)
        return "public"
    
    def _extract_table_from_ddl(self, ddl: str) -> str:
        """Extract table name from DDL"""
        import re
        match = re.search(r'CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)', ddl)
        if match:
            return match.group(2)
        return "unknown"
    
    def _create_indexes(self, cursor, metadata: Dict[str, Any], schema: str, table: str):
        """Create indexes based on metadata"""
        # Create index on primary key columns (if not already created by PK constraint)
        if metadata["primary_keys"]:
            pk_cols = ", ".join(metadata["primary_keys"])
            index_name = f"idx_{table}_pk"
            try:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON {schema}.{table} ({pk_cols})
                """)
                logger.info(f"Created index {index_name}")
            except Exception as e:
                logger.warning(f"Could not create PK index: {e}")
        
        # Create indexes on commonly queried columns
        for col in metadata["columns"]:
            if col["name"].lower() in ["id", "created_at", "updated_at", "date", "timestamp"]:
                index_name = f"idx_{table}_{col['name'].lower()}"
                try:
                    cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS {index_name} 
                        ON {schema}.{table} ({col['name']})
                    """)
                    logger.info(f"Created index {index_name}")
                except Exception as e:
                    logger.warning(f"Could not create index {index_name}: {e}")
    
    def verify_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Verify that PostgreSQL table matches Snowflake metadata"""
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        
        try:
            metadata = self.load_table_metadata(table_name)
            ddl = self.load_ddl_script(table_name)
            
            postgres_schema = self._extract_schema_from_ddl(ddl)
            postgres_table = self._extract_table_from_ddl(ddl)
            
            # Get PostgreSQL table structure
            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = '{postgres_schema}' 
                AND table_name = '{postgres_table}'
                ORDER BY ordinal_position
            """)
            
            pg_columns = cursor.fetchall()
            
            # Compare with Snowflake metadata
            # Note: PostgreSQL has 1 extra column (data_inserted_at) that's not in Snowflake
            verification = {
                "table": f"{postgres_schema}.{postgres_table}",
                "snowflake_columns": len(metadata["columns"]),
                "postgres_columns": len(pg_columns),
                "matches": True,
                "differences": []
            }
            
            # Expected: PG columns = SF columns + 1 (data_inserted_at)
            expected_pg_columns = len(metadata["columns"]) + 1
            if len(pg_columns) != expected_pg_columns:
                verification["matches"] = False
                verification["differences"].append(
                    f"Column count mismatch: SF={len(metadata['columns'])}, PG={len(pg_columns)} (expected {expected_pg_columns} with data_inserted_at)"
                )
            
            # Check individual columns (skip data_inserted_at which is first column)
            sf_cols = {col["name"]: col for col in metadata["columns"]}
            for pg_col in pg_columns:
                col_name = pg_col[0]
                # Skip the data_inserted_at column
                if col_name == "data_inserted_at":
                    continue
                if col_name not in sf_cols:
                    verification["matches"] = False
                    verification["differences"].append(f"Column {col_name} exists in PG but not SF")
            
            return verification
            
        except Exception as e:
            logger.error(f"Failed to verify table structure: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def create_all_configured_tables(self, drop_if_exists: bool = False) -> Dict[str, Any]:
        """Create all PostgreSQL tables from saved metadata"""
        import yaml
        
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        results = {}
        
        for table_config in config["tables"]:
            table_name = table_config["name"]
            
            try:
                result = self.create_table_from_metadata(table_name, drop_if_exists)
                
                # Verify table structure
                verification = self.verify_table_structure(table_name)
                result["verification"] = verification
                
                results[table_name] = result
                
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                results[table_name] = {
                    "status": "error",
                    "error": str(e)
                }
        
        return results

if __name__ == "__main__":
    loader = PostgreSQLLoader()
    results = loader.create_all_configured_tables(drop_if_exists=True)
    print("Table creation results:")
    for table, result in results.items():
        print(f"  {table}: {result['status']}")
        if result.get("verification"):
            print(f"    Verification: {'✓' if result['verification']['matches'] else '✗'}")