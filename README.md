# Morningstar Data Pipeline

A metadata-driven ETL pipeline that extracts Morningstar financial data from Snowflake and loads it into PostgreSQL. Built for **air-gapped / split-network** workflows where the Snowflake source (VPN-side) and PostgreSQL target (external-side) cannot communicate directly.

## How It Works

```
 VPN Side                         Manual Transfer                  External Side
┌──────────────────────┐        ┌──────────────────┐        ┌──────────────────────┐
│  Snowflake           │        │                  │        │  PostgreSQL          │
│                      │        │  metadata/       │        │                      │
│  1. Extract metadata ├───────►│  config/         ├───────►│  4. Create tables    │
│  2. Export data      │        │  exports/        │        │  5. Import data      │
│     (encrypted       │        │  (Parquet files)  │        │     (decrypt +       │
│      Parquet chunks) │        │                  │        │      upsert/COPY)    │
└──────────────────────┘        └──────────────────┘        └──────────────────────┘
```

1. **Configure** which tables to sync in `config/tables.yaml` — including sync mode, merge keys, and watermarks
2. **Extract metadata** — connect to Snowflake, discover schemas and primary keys, generate PostgreSQL DDL with UNIQUE constraints
3. **Export data** — query Snowflake with filters and watermarks, write chunked Parquet files, encrypt with AES-GCM
4. **Transfer** — manually copy `metadata/`, `config/`, and export directories to the PostgreSQL host
5. **Create tables** — run generated DDL against PostgreSQL (supports ALTER TABLE for schema evolution)
6. **Import data** — decrypt Parquet files and load via COPY/upsert into PostgreSQL

## Quick Start

```bash
# 1. Configure tables (sync mode, merge keys, watermarks)
#    Edit config/tables.yaml

# 2. Extract metadata from Snowflake (VPN side)
python scripts/extract_metadata.py --all

# 3. Export data to encrypted Parquet (VPN side)
python scripts/export_data.py --all

# 4. Transfer files to PostgreSQL host (manual step)

# 5. Create PostgreSQL tables (external side)
python scripts/extract_metadata.py --all --create-postgres --drop-existing

# 6. Import data into PostgreSQL (external side)
python scripts/import_data.py --all
```

## Sync Modes

Each table in `config/tables.yaml` supports one of three sync modes:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `full` | Truncate target table, then COPY all rows | Initial loads, reference data |
| `incremental` | Append rows newer than the watermark | Append-only event tables |
| `upsert` | INSERT ... ON CONFLICT DO UPDATE via staging table | Dimension tables with updates |

```yaml
tables:
  - name: "FUND_ATTRIBUTES_CA_OPENEND"
    sync_mode: "upsert"
    merge_keys: ["_ID", "_TIMESTAMPFROM"]
    watermark_column: "_TIMESTAMPFROM"
    snowflake:
      database: "PROD_DB"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_ATTRIBUTES_CA_OPENEND"
      filter:
        - "WHERE _ID IN (SELECT mstarid FROM ...)"
    postgres:
      schema: "ms"
      table: "FUND_ATTRIBUTES_CA_OPENEND"
      indexes:
        - "_ID"
        - "_TIMESTAMPFROM"
        - "_TIMESTAMPTO"
```

**`merge_keys`** drives two things automatically:

1. **Snowflake deduplication:** A `QUALIFY ROW_NUMBER() OVER (PARTITION BY {merge_keys} ORDER BY _TIMESTAMPTO DESC) = 1` clause is appended to the export query. Override the ORDER BY column with `qualify_order_by` if needed.
2. **PostgreSQL upsert:** A `UNIQUE` constraint on the merge key columns is ensured, and `INSERT ... ON CONFLICT (merge_keys) DO UPDATE` is used for loading.

**Watermark tracking:** The pipeline stores the last exported watermark in `state/{table}_watermark.json`. On subsequent runs, only rows newer than the watermark are extracted from Snowflake. If no watermark exists, the first run performs a full extraction automatically.

## Project Structure

```
ms_data_pipeline/
├── config/
│   └── tables.yaml                          # Table sync config (source/target/filters/indexes/merge_keys)
├── pipeline/
│   ├── config/
│   │   └── settings.py                      # Pydantic settings from .env
│   ├── connections/
│   │   ├── base_connection.py               # Abstract connection manager
│   │   ├── snowflake_connection.py          # Snowflake connector (SSO/password/key-pair)
│   │   └── postgres_connection.py           # PostgreSQL connector (psycopg2)
│   ├── extractors/
│   │   ├── metadata_extractor.py            # Schema + PK discovery, DDL generation from Snowflake
│   │   └── data_extractor.py                # Chunked export with WHERE/QUALIFY/watermark filtering
│   ├── loaders/
│   │   ├── postgres_loader.py               # DDL execution, ALTER TABLE evolution, migration tracking
│   │   └── data_loader.py                   # COPY FROM STDIN, upsert via staging table, chunk checkpoints
│   ├── state/
│   │   └── watermark_manager.py             # Watermark persistence for incremental sync
│   ├── transformers/
│   │   ├── encryptor.py                     # AES-GCM encryption with key versioning
│   │   ├── obfuscator.py                    # Salted deterministic IDs, secure temp files
│   │   └── type_optimizer.py                # DataFrame dtype optimization for smaller Parquet
│   └── utils/
│       ├── logger.py                        # Structured JSON logging + name sanitization filter
│       ├── ddl_generator.py                 # CREATE TABLE / UNIQUE / INDEX / ALTER TABLE generation
│       ├── metadata_comparator.py           # Schema diff with safe vs. breaking classification
│       ├── change_logger.py                 # Persists schema change history
│       ├── config_validator.py              # Validates index columns against metadata
│       ├── content_hash_comparator.py       # SHA-256 skip-if-unchanged for export chunks
│       ├── metadata_decryptor.py            # Decrypt metadata for local inspection
│       ├── run_manifest.py                  # Post-run JSON summary for auditing
│       ├── data_validator.py                # Optional per-column validation hooks
│       ├── archive.py                       # Tar.gz archiving for single-file transport
│       └── repo_manager.py                  # Disposable dataset repo management + bundle support
├── scripts/
│   ├── extract_metadata.py                  # CLI: metadata extraction + optional PG table creation
│   ├── export_data.py                       # CLI: data export with watermark + sync mode support
│   ├── import_data.py                       # CLI: decrypt + COPY/upsert into PostgreSQL with resume
│   ├── create_tables.py                     # CLI: apply DDL from metadata
│   ├── decrypt_metadata.py                  # CLI: decrypt metadata for viewing
│   ├── view_change_history.py               # CLI: inspect schema change history
│   └── compare_compression.py               # CLI: benchmark compression options
├── state/                                   # Watermarks, import checkpoints, run manifests
├── docs/                                    # Reserved for future documentation
├── environment.yml                          # Conda environment (Python 3.11)
├── pyproject.toml                           # Project metadata and tool configuration
└── .env                                     # Database credentials and settings (not committed)
```

## Key Features

### Initial Load vs. Incremental Sync

- **First run:** No watermark exists — pipeline performs a full extraction automatically
- **Subsequent runs:** Watermark from last successful export is injected into the query (`WHERE _TIMESTAMPTO > :watermark`), extracting only new/changed rows
- **Upsert on import:** Uses PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` via a temporary staging table for atomic merge operations
- **Full reload (export):** Pass `--full-reload` to ignore watermark state and re-extract all rows; the watermark is still updated after export so subsequent runs resume incrementally
- **Full reload (import):** Pass `--truncate` to truncate the PostgreSQL table before loading

### Schema Evolution

The pipeline detects schema changes between extractions and can apply them non-destructively:

- **Safe changes** (column additions, compatible type widening) are applied via `ALTER TABLE` automatically
- **Breaking changes** (column removal, type narrowing) require explicit `--force` to apply
- **Migration tracking:** All DDL changes are recorded in `{schema}._pipeline_migrations` for audit

### Compression Optimization

The pipeline applies multiple layers of compression tuning to minimize transfer size:

| Technique | Impact | Default |
|-----------|--------|---------|
| **Pre-sort by merge/watermark keys** | 20-40% smaller Parquet (better RLE and dictionary encoding) | ON |
| **Per-column dictionary encoding** | Auto-detected for low-cardinality string/category columns | ON |
| **zstd level 9** | ~20% smaller than level 3, still fast enough for batch export | Level 9 |
| **Type optimization** | Downcast ints, categorical conversion for low-cardinality strings | ON |
| **Content-hash dedup** | Skip re-encrypting unchanged chunks | ON |

Controlled via env vars (`SORT_BEFORE_COMPRESS`, `USE_DICTIONARY_ENCODING`, `COMPRESSION_LEVEL`) or CLI flags (`--no-sort`, `--no-dictionary`).

### Git Delivery: Disposable Dataset Repo

For Git-based data delivery, the pipeline uses a single disposable repository that is completely reset each run — no history is preserved. The repo acts as a fresh data delivery envelope.

```
Producer (VPN side)                          Consumer (external side)
┌──────────────────────────────┐             ┌──────────────────────────────┐
│  repos/dataset/              │  force-push │  repos/dataset/              │
│    delivery_manifest.json    │────────────>│    delivery_manifest.json    │
│    TABLE_A/  (encrypted)     │  (each run) │    TABLE_A/   shallow clone  │
│    TABLE_B/  (encrypted)     │             │    TABLE_B/                  │
└──────────────────────────────┘             └──────────────────────────────┘
         reset + commit                            clone --depth 1
```

- **Reset each run** — the repo is nuked and re-created from scratch; no history accumulates
- **Delivery manifest** — a JSON file embedded in the repo that tells the consumer exactly what to do (sync modes, merge keys, PostgreSQL targets, watermark state)
- **Self-describing** — the consumer reads the manifest and auto-imports; no `tables.yaml` needed on the PSQL side
- **Remote** — configure `DATASET_REPO_URL` pointing to GitHub, GitLab, or any Git server
- **Git bundles** — alternative `.bundle` file for fully air-gapped transfer
- **Archives** — tar.gz per table for systems without Git

See [CLI Reference](#cli-reference) for all delivery commands.

### Performance

- **PostgreSQL COPY:** Data loading uses `COPY FROM STDIN` via `copy_expert()` instead of row-by-row INSERT — typically 10-50x faster
- **Incremental queries:** Watermark support avoids re-reading the entire table from Snowflake
- **Content-hash deduplication:** Export skips re-encrypting chunks whose SHA-256 hash hasn't changed

### Security

- **AES-256-GCM encryption** with PBKDF2-HMAC-SHA256 key derivation (100k iterations)
- **Salted deterministic IDs:** Obfuscated file/folder names mix in a secret `OBFUSCATION_SALT` so they're not reproducible without the secret
- **Secure temp files:** Uses `tempfile.mkstemp()` with try/finally cleanup — no plaintext left on crash
- **Log sanitization:** When obfuscation is enabled, real table names are replaced with IDs in INFO-level logs
- **Static password:** Set `ENCRYPTION_PASSWORD` in `.env` — used by all scripts for encrypt/decrypt

### Resumable Imports

- **Chunk checkpoints:** Each successfully imported chunk is tracked in `state/{table}_import_checkpoint.json`
- **Resume on failure:** Re-running import skips already-loaded chunks instead of re-inserting them
- **Atomic upsert:** Staging table pattern ensures the real table is untouched if a merge fails

### Observability

- **Run manifests:** After each export/import, a JSON summary is saved to `state/` — tables processed, row counts, durations, errors, watermarks advanced
- **Structured logging:** Pass `structured=True` to `setup_logging()` for JSON-formatted log output
- **Data validation hooks:** Optional per-column checks (not_null, null_rate_max, min_value) configurable in `tables.yaml`

## CLI Reference

### Export Data (`scripts/export_data.py`)

```bash
# Export all tables (incremental by default — uses watermark from state/)
python scripts/export_data.py --all

# Export a single table
python scripts/export_data.py --table FUND_ATTRIBUTES_CA_OPENEND

# Full reload — ignore watermark, pull all rows (watermark still updated after)
python scripts/export_data.py --all --full-reload
python scripts/export_data.py --table FUND_ATTRIBUTES_CA_OPENEND --full-reload

# Clean — delete existing export folder before starting
python scripts/export_data.py --all --clean

# Disable name obfuscation (use real table names for folders/files)
python scripts/export_data.py --all --no-obfuscate

# Compression tuning
python scripts/export_data.py --all --no-sort --no-dictionary

# Skip repo staging (overrides REPO_MODE=repo in .env)
python scripts/export_data.py --all --repo-mode single
```

#### Git Delivery

```bash
# Commit to dataset repo + push to remote
python scripts/export_data.py --all --repo-mode repo --push

# With custom purpose in commit message / manifest
python scripts/export_data.py --all --repo-mode repo --push --purpose "Q1 refresh"

# Air-gapped — create git bundle for offline transfer
python scripts/export_data.py --all --repo-mode repo --bundle

# Archive — tar.gz per table (no git required)
python scripts/export_data.py --all --archive
```

### Import Data (`scripts/import_data.py`)

```bash
# Import all tables
python scripts/import_data.py --all

# Import a single table
python scripts/import_data.py --table FUND_ATTRIBUTES_CA_OPENEND

# Truncate + full reload into PostgreSQL
python scripts/import_data.py --all --truncate

# Pull from remote dataset repo + auto-import (reads delivery manifest)
python scripts/import_data.py --pull

# Pull + import specific table only
python scripts/import_data.py --pull --table FUND_ATTRIBUTES_CA_OPENEND

# Import from git bundle
python scripts/import_data.py --all --from-bundle bundles/dataset_20260402.bundle

# Import from archive
python scripts/import_data.py --table TABLE --from-archive exports/TABLE.tar.gz

# Keep decrypted Parquet files after import (for debugging)
python scripts/import_data.py --all --keep-decrypted
```

### Metadata & Schema (`scripts/extract_metadata.py`)

```bash
# Extract metadata from Snowflake
python scripts/extract_metadata.py --all

# Check for schema changes since last extraction
python scripts/extract_metadata.py --all --check-changes

# Create PostgreSQL tables from metadata
python scripts/extract_metadata.py --all --create-postgres --drop-existing
```

### Other Scripts

```bash
# Apply DDL to PostgreSQL
python scripts/create_tables.py --all

# Decrypt metadata for local viewing
python scripts/decrypt_metadata.py

# View schema change history
python scripts/view_change_history.py

# Benchmark compression options
python scripts/compare_compression.py
```

## Environment Setup

### 1. Create Conda Environment

```bash
conda env create -f environment.yml
conda activate ms-pipeline
```

### 2. Install Runtime Dependencies

```bash
pip install snowflake-connector-python pandas pyarrow psycopg2-binary pydantic pydantic-settings pyyaml cryptography python-dotenv
```

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Snowflake Connection (VPN Side)
SNOWFLAKE_USER=your_username
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=PROD_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_AUTH_METHOD=sso              # sso | password | key_pair

# PostgreSQL Connection (External Side)
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DATABASE=financial_data
POSTGRES_USER=your_pg_username
POSTGRES_PASSWORD=your_pg_password

# Security
ENCRYPTION_PASSWORD=your_encryption_password
OBFUSCATION_SALT=your_secret_salt      # Mixed into deterministic file/folder IDs
API_SECRET_KEY=your_api_secret_key

# Paths (defaults: exports / imports / state)
# EXPORT_BASE_DIR=exports
# IMPORT_BASE_DIR=imports
# STATE_DIR=state

# Compression optimization (defaults shown)
# COMPRESSION_LEVEL=9                 # zstd 1-22, default 9
# SORT_BEFORE_COMPRESS=true           # Pre-sort by merge keys for better encoding
# USE_DICTIONARY_ENCODING=true        # Auto-detect low-cardinality dictionary columns

# Git delivery
# REPO_MODE=single                    # single | repo
# DATASET_REPO_DIR=repos/dataset
# DATASET_REPO_URL=git@github.com:org/ms-dataset.git
# BUNDLE_OUTPUT_DIR=bundles
```

### 4. Test Connections

```bash
# Test Snowflake (requires VPN) — SSO will open a browser window
python -c "from pipeline.extractors.metadata_extractor import SnowflakeMetadataExtractor; SnowflakeMetadataExtractor().connect_to_snowflake()"

# Test PostgreSQL
python -c "from pipeline.loaders.postgres_loader import PostgreSQLLoader; PostgreSQLLoader().connect_to_postgres()"
```

## Password Management

All scripts use the `ENCRYPTION_PASSWORD` value from `.env`. Set it once and all export, import, metadata, and DDL scripts will use it automatically.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| SSO prompt appears multiple times | Pass a connection manager — scripts reuse a single SSO session across all tables |
| "Wrong password" on import | Ensure the password matches the one used during export; check `.env` |
| "Table not found in config" | Add the table to `config/tables.yaml` first |
| "Encrypted file not found" | Verify files were transferred from Snowflake server; folder names are obfuscated |
| Zero rows exported | Test your filter in Snowflake directly; check WHERE/QUALIFY logic |
| Schema change not detected | Run with `--force` to re-extract regardless of cache |
| Git repo growing too large | Repo is reset each run so this shouldn't happen; check `REPO_MODE=repo` |
| Decrypted files accidentally committed | Run `python scripts/decrypt_metadata.py --clean`; they are in `.gitignore` |

### Connection Tests

```bash
# Test Snowflake (requires VPN)
python -c "from pipeline.connections import SnowflakeConnectionManager; SnowflakeConnectionManager().connect()"

# Test PostgreSQL
python -c "from pipeline.connections import PostgresConnectionManager; PostgresConnectionManager().connect()"
```

## Git Tracking Strategy

Metadata and data live together in the **data delivery folder** (`EXPORT_BASE_DIR`, a separate git repo). The pipeline code repo contains only code and configuration.

### Pipeline Code Repo (`ms_data_pipeline`)

| File / Path | Tracked? | Reason |
|-------------|----------|--------|
| `config/tables.yaml` | Yes | Table sync configuration |
| `.env` | **No** | Contains secrets |
| `state/` | Optional | Watermarks & checkpoints |

### Data Delivery Repo (`EXPORT_BASE_DIR` / `ms_dataset`)

| File / Path | Tracked? | Reason |
|-------------|----------|--------|
| `metadata/encrypted/` | Yes | Encrypted schemas, DDL, change logs |
| `metadata/raw/` | **No** | Decrypted copies for investigation |
| `{table_folder}/*.enc` | Yes | Encrypted Parquet data chunks |
| `*.parquet` | **No** | Unencrypted intermediates |
