"""
DDL Generator
Generates PostgreSQL DDL statements including CREATE TABLE and CREATE INDEX
"""
from typing import Dict, Any, List
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


def generate_index_ddl(schema: str, table: str, column: str) -> str:
    """
    Generate a single CREATE INDEX statement.
    
    Format: CREATE INDEX idx_{table}_{column} ON {schema}.{table} ({column});
    
    Args:
        schema: PostgreSQL schema name
        table: PostgreSQL table name
        column: Column name to index
        
    Returns:
        CREATE INDEX statement
    """
    index_name = f"idx_{table}_{column}"
    return f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ({column});"


def generate_ddl_with_indexes(
    table_metadata: Dict[str, Any],
    postgres_schema: str,
    postgres_table: str,
    index_columns: List[str]
) -> str:
    """
    Generate CREATE TABLE statement followed by CREATE INDEX statements.
    
    Args:
        table_metadata: Table structure from Snowflake
        postgres_schema: Target PostgreSQL schema
        postgres_table: Target PostgreSQL table name
        index_columns: List of columns to index
        
    Returns:
        Complete DDL script with table and index creation
    """
    ddl_lines = []
    
    # Generate CREATE TABLE statement
    ddl_lines.append(f"CREATE TABLE IF NOT EXISTS {postgres_schema}.{postgres_table} (")
    
    column_definitions = []
    
    # Add insertion timestamp column as the first column
    column_definitions.append("    data_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL")
    
    # Add all original columns from Snowflake
    for col in table_metadata["columns"]:
        col_def = f"    {col['name']} {col['postgres_type']}"
        if not col['is_nullable']:
            col_def += " NOT NULL"
        if col['default_value']:
            col_def += f" DEFAULT {col['default_value']}"
        column_definitions.append(col_def)
    
    ddl_lines.append(",\n".join(column_definitions))
    
    # Add primary key constraint if exists
    if table_metadata.get("primary_keys"):
        pk_cols = ", ".join(table_metadata["primary_keys"])
        ddl_lines.append(f",\n    PRIMARY KEY ({pk_cols})")
    
    ddl_lines.append(");")
    
    # Add table comments
    ddl_lines.append(f"\n-- Source: {table_metadata['table_info']['full_name']}")
    ddl_lines.append(f"-- Extracted: {table_metadata['extracted_at']}")
    ddl_lines.append(f"-- Rows: {table_metadata['statistics']['row_count']}")
    ddl_lines.append(f"-- Note: data_inserted_at column tracks when data was inserted into PostgreSQL")
    
    # Generate CREATE INDEX statements if index columns specified
    if index_columns:
        ddl_lines.append("\n-- Indexes")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in index_columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)
        
        for col in unique_columns:
            index_ddl = generate_index_ddl(postgres_schema, postgres_table, col)
            ddl_lines.append(index_ddl)
        
        logger.info(f"Generated {len(unique_columns)} index statement(s) for table {postgres_table}")
    
    return "\n".join(ddl_lines)
