"""
PostgreSQL Data Loader
Loads data from Parquet files to PostgreSQL tables.

Supports three sync modes:
  - full:        TRUNCATE target then COPY all rows
  - incremental: COPY/append new rows (no dedup)
  - upsert:      Load into staging table then INSERT ... ON CONFLICT DO UPDATE
"""
import io
import csv
import json
import math
from pathlib import Path
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd
import psycopg2

from pipeline.config.settings import get_settings, get_postgres_connection_params
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLDataLoader:
    """Load data from Parquet files to PostgreSQL tables."""

    def __init__(self):
        self.settings = get_settings()

    def connect_to_postgres(self):
        try:
            conn_params = get_postgres_connection_params()
            conn = psycopg2.connect(**conn_params)
            logger.info(f"Connected to PostgreSQL: {self.settings.postgres_host}:{self.settings.postgres_port}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def load_parquet_to_table(
        self,
        parquet_path: Path,
        schema: str,
        table: str,
        sync_mode: str = "full",
        merge_keys: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> Dict[str, Any]:
        """Load a single Parquet chunk into a PostgreSQL table.

        Args:
            parquet_path: Path to the Parquet file.
            schema: PostgreSQL schema name.
            table: PostgreSQL table name.
            sync_mode: One of "full", "incremental", or "upsert".
            merge_keys: Column names used as conflict keys for upsert mode.
            batch_size: Rows per COPY batch (only affects memory, not SQL).
        """
        logger.info(f"Reading {parquet_path.name}")
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        total_rows = len(df)
        logger.info(f"  Rows: {total_rows:,}")

        df.columns = [col.lower() for col in df.columns]
        df = df.where(pd.notnull(df), None)

        if sync_mode == "upsert" and merge_keys:
            return self._upsert_via_staging(df, schema, table, merge_keys)
        else:
            return self._copy_load(df, schema, table)

    # ------------------------------------------------------------------
    # COPY-based fast insert (replaces iterrows + execute_batch)
    # ------------------------------------------------------------------

    def _copy_load(self, df: pd.DataFrame, schema: str, table: str) -> Dict[str, Any]:
        """Bulk load via COPY FROM STDIN — typically 10-50x faster than INSERT."""
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            columns = df.columns.tolist()
            cols_str = ", ".join(f'"{c}"' for c in columns)
            copy_sql = f'COPY {schema}.{table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')'

            buf = self._dataframe_to_csv_buffer(df)
            cursor.copy_expert(copy_sql, buf)
            conn.commit()

            logger.info(f"COPY loaded {len(df):,} rows to {schema}.{table}")
            return {"rows_loaded": len(df), "table": f"{schema}.{table}", "status": "success", "method": "copy"}

        except Exception as e:
            conn.rollback()
            logger.error(f"COPY load failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Upsert via staging table
    # ------------------------------------------------------------------

    def _upsert_via_staging(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        merge_keys: List[str],
    ) -> Dict[str, Any]:
        """Load into a temp staging table then merge into target with ON CONFLICT."""
        conn = self.connect_to_postgres()
        cursor = conn.cursor()
        staging_table = f"_staging_{table}"

        try:
            # 1. Create staging table matching target structure (no constraints)
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{staging_table}")
            cursor.execute(
                f"CREATE TABLE {schema}.{staging_table} (LIKE {schema}.{table} INCLUDING DEFAULTS)"
            )

            # 2. COPY data into staging table
            columns = df.columns.tolist()
            cols_str = ", ".join(f'"{c}"' for c in columns)
            copy_sql = (
                f'COPY {schema}.{staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')'
            )
            buf = self._dataframe_to_csv_buffer(df)
            cursor.copy_expert(copy_sql, buf)

            # 3. Merge from staging into target
            merge_keys_lower = [k.lower() for k in merge_keys]
            all_cols = [c for c in columns if c not in merge_keys_lower]
            update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in all_cols)
            conflict_cols = ", ".join(f'"{c}"' for c in merge_keys_lower)
            insert_cols = ", ".join(f'"{c}"' for c in columns)
            select_cols = ", ".join(f'"{c}"' for c in columns)

            upsert_sql = f"""
                INSERT INTO {schema}.{table} ({insert_cols})
                SELECT {select_cols} FROM {schema}.{staging_table}
                ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}
            """
            cursor.execute(upsert_sql)
            upserted = cursor.rowcount

            # 4. Cleanup
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{staging_table}")
            conn.commit()

            logger.info(f"Upserted {upserted:,} rows into {schema}.{table}")
            return {
                "rows_loaded": len(df),
                "rows_upserted": upserted,
                "table": f"{schema}.{table}",
                "status": "success",
                "method": "upsert",
            }

        except Exception as e:
            conn.rollback()
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {schema}.{staging_table}")
                conn.commit()
            except Exception:
                pass
            logger.error(f"Upsert failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dataframe_to_csv_buffer(df: pd.DataFrame) -> io.StringIO:
        """Serialize a DataFrame to an in-memory CSV buffer suitable for COPY."""
        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        for _, row in df.iterrows():
            cleaned = []
            for val in row:
                if val is None:
                    cleaned.append("\\N")
                elif isinstance(val, float) and math.isnan(val):
                    cleaned.append("\\N")
                elif pd.isna(val):
                    cleaned.append("\\N")
                else:
                    cleaned.append(str(val))
            writer.writerow(cleaned)
        buf.seek(0)
        return buf

    # ------------------------------------------------------------------
    # Verification and table operations
    # ------------------------------------------------------------------

    def verify_row_count(self, schema: str, table: str, expected_count: int) -> bool:
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            actual_count = cursor.fetchone()[0]
            matches = actual_count == expected_count

            if matches:
                logger.info(f"Row count verified: {actual_count:,} rows")
            else:
                logger.warning(f"Row count mismatch: expected {expected_count:,}, got {actual_count:,}")

            return matches
        finally:
            cursor.close()
            conn.close()

    def truncate_table(self, schema: str, table: str):
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            cursor.execute(f"TRUNCATE TABLE {schema}.{table}")
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
        conn = self.connect_to_postgres()
        cursor = conn.cursor()

        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            row_count = cursor.fetchone()[0]

            cursor.execute(
                f"SELECT pg_size_pretty(pg_total_relation_size('{schema}.{table}'))"
            )
            table_size = cursor.fetchone()[0]

            logger.info(f"Table {schema}.{table}: {row_count:,} rows, {table_size}")
            return {"row_count": row_count, "table_size": table_size}
        finally:
            cursor.close()
            conn.close()


# ---------------------------------------------------------------------------
# Chunk checkpoint for resumable imports
# ---------------------------------------------------------------------------

class ChunkCheckpoint:
    """Track which chunks have been loaded so a failed import can resume."""

    def __init__(self, checkpoint_dir: str = "state"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, table_name: str) -> Path:
        return self.checkpoint_dir / f"{table_name}_import_checkpoint.json"

    def get_loaded_chunks(self, table_name: str) -> set:
        path = self._path(table_name)
        if not path.exists():
            return set()
        try:
            with open(path, "r") as f:
                data = json.load(f)
            return set(data.get("loaded_chunks", []))
        except Exception:
            return set()

    def mark_chunk_loaded(self, table_name: str, chunk_number: int):
        loaded = self.get_loaded_chunks(table_name)
        loaded.add(chunk_number)
        with open(self._path(table_name), "w") as f:
            json.dump({"table_name": table_name, "loaded_chunks": sorted(loaded)}, f)

    def clear(self, table_name: str):
        path = self._path(table_name)
        if path.exists():
            path.unlink()
