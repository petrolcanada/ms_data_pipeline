"""
PostgreSQL Loader
Creates tables in PostgreSQL based on extracted metadata, with support for
schema evolution via ALTER TABLE and a migration tracking table.
"""
import json
import re
import yaml
import psycopg2
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from pipeline.config.settings import get_settings, get_postgres_connection_params
from pipeline.utils.logger import get_logger
from pipeline.utils.ddl_generator import classify_schema_changes, generate_alter_statements

logger = get_logger(__name__)

MIGRATION_TABLE = "_pipeline_migrations"


class PostgreSQLLoader:
    def __init__(self):
        self.settings = get_settings()
        self.metadata_dir = Path("metadata/encrypted/schemas")
        self.ddl_dir = Path("metadata/encrypted/ddl")

    def connect_to_postgres(self):
        try:
            conn_params = get_postgres_connection_params()
            conn = psycopg2.connect(**conn_params)
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    # ------------------------------------------------------------------
    # Metadata / DDL loading
    # ------------------------------------------------------------------

    def load_table_metadata(self, table_name: str) -> Dict[str, Any]:
        metadata_file = self.metadata_dir / f"{table_name}_metadata.json"
        if not metadata_file.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_file}")
        with open(metadata_file, "r") as f:
            return json.load(f)

    def load_ddl_script(self, table_name: str) -> str:
        ddl_file = self.ddl_dir / f"{table_name}_create.sql"
        if not ddl_file.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_file}")
        with open(ddl_file, "r") as f:
            return f.read()

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------

    def create_table_from_metadata(self, table_name: str, drop_if_exists: bool = False):
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            metadata = self.load_table_metadata(table_name)
            ddl = self.load_ddl_script(table_name)

            pg_schema = self._extract_schema_from_ddl(ddl)
            pg_table = self._extract_table_from_ddl(ddl)

            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {pg_schema}")

            if drop_if_exists:
                cursor.execute(f"DROP TABLE IF EXISTS {pg_schema}.{pg_table} CASCADE")
                logger.info(f"Dropped existing table {pg_schema}.{pg_table}")

            # Execute each statement separately (CREATE TABLE, ALTER TABLE, CREATE INDEX)
            for stmt in self._split_statements(ddl):
                cursor.execute(stmt)

            conn.commit()
            logger.info(f"Created table {pg_schema}.{pg_table}")

            self._ensure_migration_table(cursor, pg_schema)
            self._record_migration(
                cursor, pg_schema, pg_table,
                "create_table", ddl, f"Created with {len(metadata['columns'])} columns",
            )
            conn.commit()

            return {
                "status": "success",
                "schema": pg_schema,
                "table": pg_table,
                "columns": len(metadata["columns"]),
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Schema evolution via ALTER TABLE
    # ------------------------------------------------------------------

    def evolve_table(
        self,
        table_name: str,
        changes: List[Dict[str, Any]],
        new_metadata: Dict[str, Any],
        force: bool = False,
    ) -> Dict[str, Any]:
        """Apply safe schema changes via ALTER TABLE.

        Returns a result dict with status and details.
        """
        classified = classify_schema_changes(changes)
        safe_changes = classified["safe"]
        breaking_changes = classified["breaking"]

        if breaking_changes and not force:
            msg = (
                f"Breaking schema changes detected for {table_name}. "
                f"Use --force to apply, or --drop-existing to recreate."
            )
            logger.warning(msg)
            return {
                "status": "blocked",
                "safe_count": len(safe_changes),
                "breaking_count": len(breaking_changes),
                "breaking_changes": breaking_changes,
                "message": msg,
            }

        ddl = self.load_ddl_script(table_name)
        pg_schema = self._extract_schema_from_ddl(ddl)
        pg_table = self._extract_table_from_ddl(ddl)

        all_actionable = safe_changes + (breaking_changes if force else [])
        if not all_actionable:
            return {"status": "no_changes"}

        alter_stmts = generate_alter_statements(all_actionable, pg_schema, pg_table, new_metadata)
        if not alter_stmts:
            return {"status": "no_changes"}

        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            self._ensure_migration_table(cursor, pg_schema)

            for stmt in alter_stmts:
                logger.info(f"Executing: {stmt}")
                cursor.execute(stmt)

            summary = f"Applied {len(alter_stmts)} ALTER statement(s)"
            self._record_migration(
                cursor, pg_schema, pg_table,
                "alter_table", "\n".join(alter_stmts), summary,
            )
            conn.commit()

            logger.info(f"Schema evolution for {pg_schema}.{pg_table}: {summary}")
            return {"status": "success", "statements": alter_stmts, "summary": summary}

        except Exception as e:
            conn.rollback()
            logger.error(f"Schema evolution failed for {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Migration tracking
    # ------------------------------------------------------------------

    def _ensure_migration_table(self, cursor, schema: str):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{MIGRATION_TABLE} (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                action TEXT NOT NULL,
                ddl_executed TEXT,
                summary TEXT,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _record_migration(self, cursor, schema: str, table: str, action: str, ddl: str, summary: str):
        cursor.execute(
            f"INSERT INTO {schema}.{MIGRATION_TABLE} (table_name, action, ddl_executed, summary) "
            "VALUES (%s, %s, %s, %s)",
            (table, action, ddl, summary),
        )

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify_table_structure(self, table_name: str) -> Dict[str, Any]:
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            metadata = self.load_table_metadata(table_name)
            ddl = self.load_ddl_script(table_name)

            pg_schema = self._extract_schema_from_ddl(ddl)
            pg_table = self._extract_table_from_ddl(ddl)

            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = '{pg_schema}' AND table_name = '{pg_table}'
                ORDER BY ordinal_position
            """)

            pg_columns = cursor.fetchall()

            verification: Dict[str, Any] = {
                "table": f"{pg_schema}.{pg_table}",
                "snowflake_columns": len(metadata["columns"]),
                "postgres_columns": len(pg_columns),
                "matches": True,
                "differences": [],
            }

            expected_pg_columns = len(metadata["columns"]) + 1
            if len(pg_columns) != expected_pg_columns:
                verification["matches"] = False
                verification["differences"].append(
                    f"Column count mismatch: SF={len(metadata['columns'])}, PG={len(pg_columns)} "
                    f"(expected {expected_pg_columns} with data_inserted_at)"
                )

            sf_cols = {col["name"]: col for col in metadata["columns"]}
            for pg_col in pg_columns:
                col_name = pg_col[0]
                if col_name == "data_inserted_at":
                    continue
                if col_name not in sf_cols:
                    verification["matches"] = False
                    verification["differences"].append(f"Column {col_name} in PG but not SF")

            return verification

        except Exception as e:
            logger.error(f"Failed to verify table structure: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def create_all_configured_tables(self, drop_if_exists: bool = False) -> Dict[str, Any]:
        with open("config/tables.yaml", "r") as f:
            config = yaml.safe_load(f)

        results: Dict[str, Any] = {}
        for table_config in config["tables"]:
            table_name = table_config["name"]
            try:
                result = self.create_table_from_metadata(table_name, drop_if_exists)
                result["verification"] = self.verify_table_structure(table_name)
                results[table_name] = result
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                results[table_name] = {"status": "error", "error": str(e)}

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_schema_from_ddl(ddl: str) -> str:
        match = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)", ddl)
        return match.group(1) if match else "public"

    @staticmethod
    def _extract_table_from_ddl(ddl: str) -> str:
        match = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)", ddl)
        return match.group(2) if match else "unknown"

    @staticmethod
    def _split_statements(ddl: str) -> List[str]:
        """Split a DDL script into individual executable statements."""
        stmts: List[str] = []
        current: List[str] = []
        for line in ddl.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            current.append(line)
            if stripped.endswith(";"):
                stmts.append("\n".join(current))
                current = []
        if current:
            stmts.append("\n".join(current))
        return stmts


if __name__ == "__main__":
    loader = PostgreSQLLoader()
    results = loader.create_all_configured_tables(drop_if_exists=True)
    print("Table creation results:")
    for table, result in results.items():
        print(f"  {table}: {result['status']}")
        if result.get("verification"):
            v = result["verification"]
            print(f"    Verification: {'OK' if v['matches'] else 'MISMATCH'}")
