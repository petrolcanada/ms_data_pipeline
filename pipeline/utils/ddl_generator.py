"""
DDL Generator
Generates PostgreSQL DDL statements including CREATE TABLE, CREATE INDEX,
UNIQUE constraints for merge keys, and ALTER TABLE for schema evolution.
"""
from typing import Dict, Any, List, Optional
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


def generate_index_ddl(schema: str, table: str, column: str) -> str:
    """Generate a single CREATE INDEX IF NOT EXISTS statement."""
    index_name = f"idx_{table}_{column}"
    return f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ({column});"


def generate_unique_constraint_ddl(schema: str, table: str, columns: List[str]) -> str:
    """Generate a UNIQUE constraint for merge/upsert keys."""
    constraint_name = f"uq_{table}_{'_'.join(c.lower() for c in columns)}"
    cols_str = ", ".join(columns)
    return f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint_name} UNIQUE ({cols_str});"


def generate_ddl_with_indexes(
    table_metadata: Dict[str, Any],
    postgres_schema: str,
    postgres_table: str,
    index_columns: List[str],
    merge_keys: Optional[List[str]] = None,
) -> str:
    """
    Generate CREATE TABLE + CREATE INDEX + optional UNIQUE constraint DDL.

    Args:
        table_metadata: Table structure from Snowflake
        postgres_schema: Target PostgreSQL schema
        postgres_table: Target PostgreSQL table name
        index_columns: List of columns to index
        merge_keys: Columns used as upsert keys (emitted as UNIQUE constraint)
    """
    ddl_lines: List[str] = []

    ddl_lines.append(f"CREATE TABLE IF NOT EXISTS {postgres_schema}.{postgres_table} (")

    column_definitions: List[str] = []
    column_definitions.append("    data_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL")

    for col in table_metadata["columns"]:
        col_def = f"    {col['name']} {col['postgres_type']}"
        if not col["is_nullable"]:
            col_def += " NOT NULL"
        if col["default_value"]:
            col_def += f" DEFAULT {col['default_value']}"
        column_definitions.append(col_def)

    ddl_lines.append(",\n".join(column_definitions))

    if table_metadata.get("primary_keys"):
        pk_cols = ", ".join(table_metadata["primary_keys"])
        ddl_lines.append(f",\n    PRIMARY KEY ({pk_cols})")

    ddl_lines.append(");")

    ddl_lines.append(f"\n-- Source: {table_metadata['table_info']['full_name']}")
    ddl_lines.append(f"-- Extracted: {table_metadata['extracted_at']}")
    ddl_lines.append(f"-- Rows: {table_metadata['statistics']['row_count']}")
    ddl_lines.append("-- Note: data_inserted_at column tracks when data was inserted into PostgreSQL")

    # UNIQUE constraint for merge/upsert keys (only when no PK already covers them)
    if merge_keys and not table_metadata.get("primary_keys"):
        ddl_lines.append("\n-- Unique constraint for upsert merge keys")
        ddl_lines.append(
            generate_unique_constraint_ddl(postgres_schema, postgres_table, merge_keys)
        )
        logger.info(f"Generated UNIQUE constraint on ({', '.join(merge_keys)}) for {postgres_table}")

    if index_columns:
        ddl_lines.append("\n-- Indexes")
        seen: set = set()
        unique_columns: List[str] = []
        for col in index_columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)

        for col in unique_columns:
            ddl_lines.append(generate_index_ddl(postgres_schema, postgres_table, col))

        logger.info(f"Generated {len(unique_columns)} index statement(s) for table {postgres_table}")

    return "\n".join(ddl_lines)


# ---------------------------------------------------------------------------
# Schema evolution helpers
# ---------------------------------------------------------------------------

def classify_schema_changes(changes: List[Dict[str, Any]]) -> Dict[str, List[Dict]]:
    """Classify metadata changes as safe (additive) or breaking (destructive).

    Returns {"safe": [...], "breaking": [...]}.
    """
    safe: List[Dict] = []
    breaking: List[Dict] = []

    for change in changes:
        ctype = change.get("type", "")
        if ctype in ("column_added",):
            safe.append(change)
        elif ctype in ("column_type_changed",):
            details = change.get("details", {})
            if _is_compatible_type_widening(details.get("old_type"), details.get("new_type")):
                safe.append(change)
            else:
                breaking.append(change)
        elif ctype in ("column_nullable_changed",):
            details = change.get("details", {})
            if details.get("new_nullable") and not details.get("old_nullable"):
                safe.append(change)
            else:
                breaking.append(change)
        else:
            breaking.append(change)

    return {"safe": safe, "breaking": breaking}


def generate_alter_statements(
    changes: List[Dict[str, Any]],
    postgres_schema: str,
    postgres_table: str,
    table_metadata: Dict[str, Any],
) -> List[str]:
    """Generate ALTER TABLE statements for safe/additive schema changes.

    Only handles column additions and compatible type widening.
    """
    stmts: List[str] = []
    col_lookup = {c["name"]: c for c in table_metadata.get("columns", [])}
    fqn = f"{postgres_schema}.{postgres_table}"

    for change in changes:
        ctype = change.get("type", "")
        if ctype == "column_added":
            col_name = change["column"]
            col_info = col_lookup.get(col_name)
            if col_info:
                pg_type = col_info["postgres_type"]
                nullable = "" if col_info.get("is_nullable") else " NOT NULL"
                default = f" DEFAULT {col_info['default_value']}" if col_info.get("default_value") else ""
                stmts.append(f"ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {col_name} {pg_type}{nullable}{default};")
        elif ctype == "column_type_changed":
            col_name = change["column"]
            col_info = col_lookup.get(col_name)
            if col_info:
                pg_type = col_info["postgres_type"]
                stmts.append(f"ALTER TABLE {fqn} ALTER COLUMN {col_name} TYPE {pg_type};")
        elif ctype == "column_nullable_changed":
            col_name = change["column"]
            details = change.get("details", {})
            if details.get("new_nullable"):
                stmts.append(f"ALTER TABLE {fqn} ALTER COLUMN {col_name} DROP NOT NULL;")

    return stmts


def _is_compatible_type_widening(old_type: Optional[str], new_type: Optional[str]) -> bool:
    """Return True if old_type -> new_type is a safe widening."""
    if not old_type or not new_type:
        return False
    old_upper = old_type.upper()
    new_upper = new_type.upper()
    safe_widenings = {
        "SMALLINT": {"INTEGER", "BIGINT", "NUMERIC"},
        "INTEGER": {"BIGINT", "NUMERIC"},
        "BIGINT": {"NUMERIC"},
        "REAL": {"DOUBLE PRECISION"},
    }
    if new_upper in safe_widenings.get(old_upper, set()):
        return True
    if old_upper.startswith("VARCHAR") and new_upper.startswith("VARCHAR"):
        try:
            old_len = int(old_upper.split("(")[1].rstrip(")"))
            new_len = int(new_upper.split("(")[1].rstrip(")"))
            return new_len >= old_len
        except (IndexError, ValueError):
            pass
    if old_upper.startswith("VARCHAR") and new_upper == "TEXT":
        return True
    return False
