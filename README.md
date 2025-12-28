# Data Pipeline System

This repository contains a Python-based data management system for processing financial data from Snowflake to PostgreSQL.

## How It Works

### 1. Configure Tables to Sync
Edit `config/tables.yaml` to specify which Snowflake tables you want to sync:

```yaml
tables:
  - name: "financial_data"
    snowflake:
      database: "PROD_DB"
      schema: "PUBLIC" 
      table: "FINANCIAL_DATA"
      # Optional: Filter data during export
      filter:
        - "WHERE SECID IN (SELECT SECID FROM PUBLIC.VW_ACTIVE_FUNDS)"
        - "AND _TIMESTAMPTO LIKE '%9999%'"
    postgres:
      schema: "public"
      table: "financial_data"
    sync_mode: "full"
    schedule: "daily"
    encryption: true
    compression: true
```

**Filter Options:**
- **No filter**: Omit the `filter` field to export all data
- **Single filter**: `filter: "WHERE column = value"`
- **Multiple filters**: Use a list to combine conditions with AND
  ```yaml
  filter:
    - "WHERE id IN (SELECT id FROM view)"
    - "AND status = 'ACTIVE'"
    - "AND date > '2024-01-01'"
  ```

### 2. Extract Metadata from Snowflake (VPN Side)
Run the metadata extraction script to connect to Snowflake and extract table schemas:

```bash
python scripts/extract_metadata.py --all
```

This will:
- Connect to Snowflake (requires VPN access)
- Extract table schemas, column types, constraints
- Map Snowflake types to PostgreSQL equivalents
- Save metadata to `metadata/schemas/{table}_metadata.json`
- Generate PostgreSQL DDL and save to `metadata/ddl/{table}_create.sql`

### 3. Create PostgreSQL Tables (External Side)
The same script can also create the PostgreSQL tables:

```bash
python scripts/extract_metadata.py --all --create-postgres --drop-existing
```

This will:
- Read the saved metadata files
- Connect to PostgreSQL (external, no VPN needed)
- Execute the DDL to create tables
- Create appropriate indexes
- Verify table structure matches Snowflake

### 4. Repository Structure After Metadata Extraction

```
financial-data-management/
├── config/
│   └── tables.yaml                    # Your table configuration
├── metadata/
│   ├── schemas/                       # Extracted metadata (JSON)
│   │   ├── financial_data_metadata.json
│   │   ├── market_prices_metadata.json
│   │   └── company_info_metadata.json
│   └── ddl/                          # Generated PostgreSQL DDL
│       ├── financial_data_create.sql
│       ├── market_prices_create.sql
│       └── company_info_create.sql
└── pipeline/
    ├── extractors/
    │   └── metadata_extractor.py     # Connects to Snowflake, extracts schemas
    └── loaders/
        └── postgres_loader.py        # Creates PostgreSQL tables from metadata
```

## Key Benefits

1. **Metadata-Driven**: Just specify table names, the system discovers schemas automatically
2. **Type Mapping**: Automatically converts Snowflake types to PostgreSQL equivalents
3. **Version Controlled**: All metadata and DDL saved in the repository
4. **Cross-Network**: Metadata extraction on VPN side, table creation on external side
5. **Verification**: Automatically verifies PostgreSQL tables match Snowflake schemas
6. **Name Obfuscation**: Optional feature to randomize folder and file names for enhanced security

## Example Workflow

```bash
# 1. Configure your tables
vim config/tables.yaml

# 2. Extract metadata from Snowflake (run on VPN side)
python scripts/extract_metadata.py --all --check-changes

# 3. Export data (optionally with name obfuscation)
python scripts/export_data.py --all --obfuscate

# 4. Create PostgreSQL tables (can run on external side)
python scripts/extract_metadata.py --all --create-postgres

# 5. Import data
python scripts/import_data.py --all

# 6. Verify everything worked
ls metadata/schemas/  # Check metadata files
ls metadata/ddl/      # Check DDL files
```

## Metadata Change Tracking

Monitor table schema changes over time with automatic detection and alerting:

```bash
# Check for metadata changes
python scripts/extract_metadata.py --all --check-changes
```

**What it tracks:**
- Column additions/removals
- Data type changes
- NULL constraint changes
- Column position changes
- Constraint modifications

**When changes are detected:**
- Displays detailed alert with all changes
- Archives old metadata with timestamp: `{table}_{YYYYMMDD}_metadata.json`
- Archives old DDL with timestamp: `{table}_{YYYYMMDD}_create.sql`
- Logs changes to: `metadata/changes/{table}_changes.log`
- Updates current files with new metadata

**Example alert:**
```
⚠️  METADATA CHANGES DETECTED!
Table: FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
Summary: 1 column added

Detailed Changes:
  + Column added: RISK_RATING (VARCHAR(50))

Archived old metadata:
  • metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_metadata.json
  • metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_create.sql
```

See [Metadata Change Tracking Guide](docs/metadata-change-tracking.md) for details.

## Name Obfuscation (Optional Security Feature)

For enhanced security, you can enable name obfuscation to randomize folder and file names:

```bash
# Export with obfuscated names
python scripts/export_data.py --all --obfuscate
```

**What it does:**
- Replaces table names with random IDs (e.g., `a7f3d9e2c4b8f1a9`)
- Replaces file names with random IDs (e.g., `b4c8f1a9.enc`)
- Creates encrypted master index (`.export_index.enc`)
- Import automatically detects and handles obfuscated exports

**Benefits:**
- Casual observers cannot identify table names
- File listings reveal no information about data structure
- Adds extra security layer on top of encryption

See [Name Obfuscation Guide](docs/name-obfuscation-guide.md) for details.

## Next Steps

After metadata extraction and table creation:
1. Run the actual data pipeline to sync data
2. Set up scheduling for regular syncs
3. Monitor pipeline execution
4. Use the REST API to access data

## Environment Setup

### 1. Create Conda Environment
```bash
# Create environment from file
conda env create -f environment.yml

# Or for development with extra tools
conda env create -f environment-dev.yml

# Activate environment
conda activate data-pipeline-system
```

### 2. Alternative: Manual Environment Creation
```bash
# Create environment with Python version
conda create -n financial-data-management python=3.11

# Activate environment
conda activate data-pipeline-system

# Install dependencies
conda env update -f environment.yml
```

### 2. Configure Database Connections
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your actual credentials
vim .env
```

**Required Environment Variables for SSO:**
```bash
# Snowflake Connection (VPN Side) - SSO Authentication
SNOWFLAKE_USER=your_username
SNOWFLAKE_ACCOUNT=your_account.region  # e.g., abc123.us-east-1
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=PROD_DB
SNOWFLAKE_SCHEMA=PUBLIC

# Authentication method
SNOWFLAKE_AUTH_METHOD=sso

# Optional: Your SSO provider URL (if different from default)
SNOWFLAKE_SSO_URL=https://your-company.okta.com

# PostgreSQL Connection (External Side)
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DATABASE=financial_data
POSTGRES_USER=your_pg_username
POSTGRES_PASSWORD=your_pg_password
```

**Alternative Authentication Methods:**
```bash
# For password authentication
SNOWFLAKE_AUTH_METHOD=password
SNOWFLAKE_PASSWORD=your_password

# For key pair authentication
SNOWFLAKE_AUTH_METHOD=key_pair
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase  # Optional
```

### 3. Test Connections
```bash
# Ensure conda environment is activated
conda activate data-pipeline-system

# Test Snowflake SSO connection (requires VPN)
# This will open a browser window for SSO authentication
python -c "from pipeline.extractors.metadata_extractor import SnowflakeMetadataExtractor; SnowflakeMetadataExtractor().connect_to_snowflake()"

# Test PostgreSQL connection
python -c "from pipeline.loaders.postgres_loader import PostgreSQLLoader; PostgreSQLLoader().connect_to_postgres()"
```

**SSO Authentication Notes:**
- When using SSO, the first connection will open your default browser
- Complete the authentication in the browser window
- The connection will be established after successful SSO login
- Subsequent connections may use cached credentials (depending on your SSO setup)

## Conda Environment Management

### **Environment Files:**
- **`environment.yml`** - Production dependencies
- **`environment-dev.yml`** - Development dependencies (includes testing, linting, Jupyter)

### **Common Conda Commands:**
```bash
# List environments
conda env list

# Update environment from file
conda env update -f environment.yml

# Export current environment
conda env export > environment-backup.yml

# Remove environment
conda env remove -n data-pipeline-system

# Install additional packages
conda install package-name
# or
pip install package-name  # for packages not in conda
```

### **Development Tools Included:**
- **Testing**: pytest, pytest-cov, pytest-asyncio
- **Code Quality**: black, isort, flake8, mypy, pre-commit
- **Documentation**: sphinx, sphinx-rtd-theme
- **Data Exploration**: jupyter, matplotlib, seaborn
- **Database Tools**: pgcli for PostgreSQL CLI




Metadata:
snowflake export into the current repo in metadata folder including raw schema and ddl
manual process to accessible in the psql server
a process to loop through the ddl and create table if not exists

actual data:
initial load:
data export from snowflake into a dedicated local file directory
a manual process to move the files to the psql server
psql server load the data into psql from a dedicated file directy on the psql server
