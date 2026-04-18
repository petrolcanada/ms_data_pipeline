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

### First-Time Setup

> Complete steps 1-6 in order. Steps 2-3 run on the **VPN-side machine** (Snowflake access). Steps 5-6 run on the **external-side machine** (PostgreSQL access).

**1. Configure tables**

Edit `config/tables.yaml` to define which tables to sync, their sync mode, merge keys, and filters.

**2. Extract metadata from Snowflake** _(VPN side)_

```bash
python scripts/extract_metadata.py --all
```

Connects to Snowflake, discovers schemas and primary keys, and generates PostgreSQL DDL.

**3. Export data** _(VPN side)_

```bash
python scripts/export_data.py --all
```

Queries Snowflake, writes chunked Parquet files, and encrypts them with AES-GCM.

**4. Transfer files** _(manual)_

Copy the `metadata/`, `config/`, and export directories from the VPN-side machine to the PostgreSQL host. For Git-based delivery, see [Git Delivery](#git-delivery).

**5. Create PostgreSQL tables** _(external side)_

```bash
python scripts/create_tables.py --all
```

Reads transferred DDL from `IMPORT_BASE_DIR` and runs it against PostgreSQL. Only needed on first run or when schema changes are detected. Add `--drop-existing` to recreate tables.

**6. Import data** _(external side)_

```bash
python scripts/import_data.py --all
```

Decrypts Parquet files and loads them into PostgreSQL via COPY or upsert (depending on sync mode).

---

### Common Workflows

The pipeline is air-gapped: Snowflake is only reachable from the VPN side, PostgreSQL is only reachable from the external side. Every workflow follows the same pattern: **export on VPN side → transfer → import on external side**.

> **Note:** `export_data.py` automatically extracts metadata (schema + DDL) before exporting data. You never need to run `extract_metadata.py` separately unless you want metadata only without exporting data.
>
> When a table has a `source_query`, the pipeline derives metadata from the actual query output rather than the raw Snowflake table. This applies to **all** tables with `source_query` — not just VARIANT/JSON unpacking. For example, tables that join with `month_ends` to add a `MONTHENDDATE` column, or alias an existing column (`SELECT *, RATINGDATE AS MONTHENDDATE`), will also have their metadata reflect the query output so that the generated DDL, indexes, and UNIQUE constraints match what the export actually produces.

#### 1. Adding a Brand New Table

The PostgreSQL table does not exist yet — you must create it before importing data.

**Define the table in `config/tables.yaml`:**

```yaml
tables:
  - name: "MY_NEW_TABLE"
    sync_mode: "upsert"
    merge_keys: ["_ID", "MONTHENDDATE"]
    watermark_column: "_TIMESTAMPFROM"
    snowflake:
      <<: *sf_defaults
      table: "MY_NEW_TABLE"
      source_query: |        # optional — only if the raw table needs transformation
        SELECT *, SOME_DATE AS MONTHENDDATE
        FROM {table}
      filter:
        - *peer_filter        # optional
    postgres:
      schema: "ms"
      table: "MY_NEW_TABLE"
      indexes:
        - "_ID"
        - "MONTHENDDATE"
        - "_TIMESTAMPFROM"
        - "_TIMESTAMPTO"
```

**VPN side:**

```bash
python scripts/export_data.py --table MY_NEW_TABLE --push
```

**External side** (git delivery):

```bash
python scripts/import_data.py --pull                    # pull metadata + data from remote
python scripts/create_tables.py --table MY_NEW_TABLE    # create the PostgreSQL table from DDL
python scripts/import_data.py --table MY_NEW_TABLE      # decrypt + load data
```

**External side** (manual transfer):

```bash
# (after copying metadata/ and data/ to PostgreSQL host)
python scripts/create_tables.py --table MY_NEW_TABLE
python scripts/import_data.py --table MY_NEW_TABLE
```

After this, the table is part of the regular sync cycle — subsequent `--all` runs include it automatically.

#### 2. Day-to-Day Incremental Sync

No schema changes. PostgreSQL tables already exist. The pipeline uses watermarks to extract only new/changed rows.

**VPN side:**

```bash
python scripts/export_data.py --all --push
```

**External side:**

```bash
python scripts/import_data.py --pull
```

Or without git delivery:

```bash
# VPN side
python scripts/export_data.py --all
# (manually transfer files)
# External side
python scripts/import_data.py --all
```

#### 3. Schema Changed — Columns Added or Removed

The import handles both cases automatically. No manual DDL step needed.

- **Columns added in Snowflake** — `import_data.py` detects columns present in the incoming data but missing from PostgreSQL and runs `ALTER TABLE ADD COLUMN` automatically.
- **Columns removed from Snowflake** — the import only references columns in the incoming data (via explicit `COPY ... (col1, col2, ...)`). Old columns in PostgreSQL are left in place — new rows get NULLs for those columns, existing data stays untouched.

**VPN side:**

```bash
python scripts/export_data.py --all --push
```

**External side:**

```bash
python scripts/import_data.py --pull
```

Same commands as day-to-day. The pipeline logs which columns were added.

#### 4. Schema Changed — Incompatible Type Change

This is the only scenario that requires dropping and recreating the PostgreSQL table: when a column's data type changed and PostgreSQL cannot auto-cast the values (e.g. `TEXT` → `INTEGER`). This is rare.

**VPN side:**

```bash
python scripts/export_data.py --table AFFECTED_TABLE --full-reload --push
```

**External side:**

```bash
python scripts/import_data.py --pull                                            # pull updated metadata + data
python scripts/create_tables.py --table AFFECTED_TABLE --drop-existing          # drop + recreate from new DDL
python scripts/import_data.py --table AFFECTED_TABLE                            # reload all data
```

#### 5. Full Reload (Re-export Everything)

Ignore watermarks and re-extract all rows from Snowflake. Useful to reset state or after major changes.

**VPN side:**

```bash
python scripts/export_data.py --all --full-reload --push
```

**External side:**

```bash
python scripts/import_data.py --pull --truncate
```

`--full-reload` tells Snowflake to ignore the watermark and pull all rows. `--truncate` clears the PostgreSQL tables before loading so you don't get duplicates.

#### Quick Reference

| Scenario | VPN side | External side |
|---|---|---|
| **New table** | `export_data.py --table X --push` | `import_data.py --pull` then `create_tables.py --table X` then `import_data.py --table X` |
| **Incremental sync** | `export_data.py --all --push` | `import_data.py --pull` |
| **Columns added/removed** | `export_data.py --all --push` | `import_data.py --pull` (auto ALTER TABLE) |
| **Incompatible type change** | `export_data.py --table X --full-reload --push` | `import_data.py --pull` then `create_tables.py --table X --drop-existing` then `import_data.py --table X` |
| **Full reload** | `export_data.py --all --full-reload --push` | `import_data.py --pull --truncate` |
| **Metadata only (no data)** | `extract_metadata.py --table X` | `create_tables.py --table X` |

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
│   ├── extract_metadata.py                  # CLI: metadata extraction from Snowflake (VPN side)
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

The pipeline detects schema changes between extractions and handles them on import:

- **Columns added** — `import_data.py` runs `ALTER TABLE ADD COLUMN` automatically
- **Columns removed** — import continues normally; old columns are left in PostgreSQL with NULLs for new rows
- **Incompatible type changes** — requires `create_tables.py --drop-existing` to recreate the table (rare)
- **Migration tracking:** DDL changes are recorded in `{schema}._pipeline_migrations` for audit

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

### Git Delivery

The same Git repo lives on both sides — `EXPORT_BASE_DIR` on the VPN host and `IMPORT_BASE_DIR` on the PSQL host.

```
VPN side (EXPORT_BASE_DIR)                   PSQL side (IMPORT_BASE_DIR)
┌──────────────────────────────┐             ┌──────────────────────────────┐
│  ms_dataset/                 │  git push   │  ms_dataset_init/            │
│    data/encrypted/           │────────────>│    data/encrypted/           │
│    metadata/encrypted/       │             │    metadata/encrypted/       │
│    delivery_manifest.enc     │             │    delivery_manifest.enc     │
└──────────────────────────────┘             └──────────────────────────────┘
      export -> commit -> push                       git pull -> import
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

```

#### Git Delivery

Every export automatically commits to `EXPORT_BASE_DIR`. Add `--push` to push the commit to the remote.

```bash
# Export + commit + push
python scripts/export_data.py --all --push

# With custom purpose in commit message / manifest
python scripts/export_data.py --all --push --purpose "Q1 refresh"

# Air-gapped — create git bundle for offline transfer
python scripts/export_data.py --all --bundle

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

### Metadata & Schema

```bash
# Extract metadata from Snowflake (VPN side)
python scripts/extract_metadata.py --all

# Extract a single table
python scripts/extract_metadata.py --table FUND_ATTRIBUTES_CA_OPENEND

# Create PostgreSQL tables from transferred DDL (external side)
python scripts/create_tables.py --all
python scripts/create_tables.py --table FUND_ATTRIBUTES_CA_OPENEND
python scripts/create_tables.py --all --drop-existing
```

### Other Scripts

```bash
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
# DATASET_REPO_URL=git@github.com:org/ms-dataset.git
# BUNDLE_OUTPUT_DIR=bundles
```

### 4. Test Connections

```bash
# Test Snowflake (requires VPN) — SSO will open a browser window
python -c "from pipeline.connections import SnowflakeConnectionManager; SnowflakeConnectionManager().connect()"

# Test PostgreSQL
python -c "from pipeline.connections import PostgresConnectionManager; PostgresConnectionManager().connect()"
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
| Schema change not detected | Re-run `extract_metadata.py` — it always re-extracts from Snowflake |
| Git repo growing too large | Run `git gc` in the dataset repo, or add large generated files to `.gitignore` |
| Decrypted files accidentally committed | Run `python scripts/decrypt_metadata.py --clean`; they are in `.gitignore` |

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
