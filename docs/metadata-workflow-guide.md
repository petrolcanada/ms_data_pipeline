# Metadata Management Workflow Guide

This guide shows you the complete workflow for extracting, tracking, encrypting, and viewing metadata from Snowflake.

## Prerequisites

1. **Environment Setup**: Ensure `.env` file is configured
   ```bash
   # Required in .env
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   SNOWFLAKE_ROLE=your_role
   
   # For encryption/obfuscation
   ENCRYPTION_PASSWORD=your_strong_password
   OBFUSCATE_NAMES=true
   ```

2. **Table Configuration**: Configure tables in `config/tables.yaml`
   ```yaml
   tables:
     - name: "financial_data"
       snowflake:
         database: "PROD_DB"
         schema: "PUBLIC"
         table: "FINANCIAL_DATA"
       postgres:
         schema: "public"
         table: "financial_data"
   ```

## Workflow Scenarios

### Scenario 1: First-Time Metadata Extraction (With Obfuscation)

**Step 1: Extract metadata from Snowflake**
```bash
# Extract all tables (obfuscation & change tracking enabled by default)
python scripts/extract_metadata.py --all

# What happens:
# - Connects to Snowflake
# - Extracts table schemas
# - Generates PostgreSQL DDL
# - Encrypts metadata files
# - Encrypts change logs
# - Creates master index
```

**Output:**
```
metadata/
├── schemas/
│   ├── 4923cba5118f2c90.enc          # Encrypted metadata
│   └── 7b371a289b3b1fef.enc
├── ddl/
│   ├── 18a094ce60d6f8ed.enc          # Encrypted DDL
│   └── 3f2c94139c5434a6.enc
├── changes/
│   ├── a1b2c3d4e5f6g7h8.enc          # Encrypted change log
│   └── i9j0k1l2m3n4o5p6.enc
└── index.enc                          # Master index
```

**Step 2: Decrypt metadata for viewing (optional)**
```bash
# Decrypt all tables (uses password from .env)
python scripts/decrypt_metadata.py --all

# What happens:
# - Reads master index
# - Decrypts all metadata files
# - Decrypts all DDL files
# - Decrypts all change logs
# - Saves to metadata/decrypted/
```

**Output:**
```
metadata/decrypted/
├── schemas/
│   ├── FINANCIAL_DATA_metadata.json
│   └── MARKET_DATA_metadata.json
├── ddl/
│   ├── FINANCIAL_DATA_create.sql
│   └── MARKET_DATA_create.sql
└── changes/
    ├── FINANCIAL_DATA_changes.log
    └── MARKET_DATA_changes.log
```

**Step 3: View decrypted files**
```bash
# View metadata JSON
cat metadata/decrypted/schemas/FINANCIAL_DATA_metadata.json

# View DDL
cat metadata/decrypted/ddl/FINANCIAL_DATA_create.sql

# View change history
cat metadata/decrypted/changes/FINANCIAL_DATA_changes.log
```

**Step 4: Clean up decrypted files**
```bash
# Delete all decrypted files (keeps encrypted files safe)
python scripts/decrypt_metadata.py --clean
```

---

### Scenario 2: Subsequent Metadata Extraction (Detecting Changes)

**Step 1: Extract metadata again (after schema changes in Snowflake)**
```bash
# Extract with change tracking (enabled by default)
python scripts/extract_metadata.py --all

# What happens:
# - Extracts current metadata
# - Decrypts previous metadata
# - Compares old vs new
# - Detects changes (columns added/removed/modified)
# - Archives old encrypted files with timestamp
# - Encrypts new metadata
# - Logs changes to encrypted change log
```

**Console Output:**
```
[2024-01-03T10:30:45Z] Schema change detected for FINANCIAL_DATA
Summary: 2 columns added, 1 type changed
  • Column added: NEW_COLUMN (VARCHAR(100), NOT NULL)
  • Column added: ANOTHER_COLUMN (INTEGER, NULL)
  • Column type changed: AMOUNT (NUMBER(18,2) → NUMERIC(18,2))
Archived: metadata/schemas/4923cba5118f2c90_20240103.enc
Archived: metadata/ddl/18a094ce60d6f8ed_20240103.enc
```

**File Structure After Change:**
```
metadata/
├── schemas/
│   ├── 4923cba5118f2c90.enc          # Current (new)
│   ├── 4923cba5118f2c90_20240103.enc # Archived (old)
│   └── 7b371a289b3b1fef.enc
├── ddl/
│   ├── 18a094ce60d6f8ed.enc          # Current (new)
│   ├── 18a094ce60d6f8ed_20240103.enc # Archived (old)
│   └── 3f2c94139c5434a6.enc
└── changes/
    ├── a1b2c3d4e5f6g7h8.enc          # Updated with new entry
    └── i9j0k1l2m3n4o5p6.enc
```

**Step 2: View change history (without decrypting all files)**
```bash
# View change history for specific table (obfuscation enabled by default)
python scripts/view_change_history.py --table FINANCIAL_DATA

# What happens:
# - Uses password from .env
# - Decrypts change log temporarily
# - Displays change history
# - Removes temporary decrypted file
```

**Output:**
```
================================================================================
CHANGE HISTORY: FINANCIAL_DATA
================================================================================

Showing all 2 entries (most recent first)

Entry 1:
[2024-01-03T10:30:45Z] Schema change detected
Summary: 2 columns added, 1 type changed

Changes:
  + Column added: NEW_COLUMN (VARCHAR(100), NOT NULL)
  + Column added: ANOTHER_COLUMN (INTEGER, NULL)
  ~ Column type changed: AMOUNT
      Old: NUMBER(18,2)
      New: NUMERIC(18,2)

Archived Files:
  - metadata/schemas/4923cba5118f2c90_20240103.enc
  - metadata/ddl/18a094ce60d6f8ed_20240103.enc

================================================================================

Entry 2:
[2024-01-01T08:15:30Z] Initial metadata extraction

Created Files:
  - metadata/schemas/4923cba5118f2c90.enc
  - metadata/ddl/18a094ce60d6f8ed.enc

================================================================================
```

---

### Scenario 3: Non-Obfuscated Mode (Plain Text)

**Step 1: Extract without obfuscation**
```bash
# Extract without obfuscation (plain text files)
python scripts/extract_metadata.py --all --no-obfuscate

# Note: No obfuscation, no password needed, but change tracking still enabled
```

**Output:**
```
metadata/
├── schemas/
│   ├── FINANCIAL_DATA_metadata.json
│   └── MARKET_DATA_metadata.json
├── ddl/
│   ├── FINANCIAL_DATA_create.sql
│   └── MARKET_DATA_create.sql
└── changes/
    ├── FINANCIAL_DATA_changes.log
    └── MARKET_DATA_changes.log
```

**Step 2: View files directly (no decryption needed)**
```bash
# View metadata
cat metadata/schemas/FINANCIAL_DATA_metadata.json

# View DDL
cat metadata/ddl/FINANCIAL_DATA_create.sql

# View change history
python scripts/view_change_history.py --table FINANCIAL_DATA
# Or directly:
cat metadata/changes/FINANCIAL_DATA_changes.log
```

---

### Scenario 4: Selective Operations

**Decrypt specific table only:**
```bash
# Decrypt just one table
python scripts/decrypt_metadata.py --table FINANCIAL_DATA

# Output:
# metadata/decrypted/schemas/FINANCIAL_DATA_metadata.json
# metadata/decrypted/ddl/FINANCIAL_DATA_create.sql
# metadata/decrypted/changes/FINANCIAL_DATA_changes.log
```

**View limited change history:**
```bash
# View last 5 changes
python scripts/view_change_history.py --table FINANCIAL_DATA --limit 5

# View changes in date range
python scripts/view_change_history.py --table FINANCIAL_DATA \
  --from 2024-01-01 --to 2024-01-31
```

**List available tables:**
```bash
# List all tables in master index
python scripts/decrypt_metadata.py --list
```

**View summary of all tables:**
```bash
# Show which tables have change logs
python scripts/view_change_history.py --summary
```

---

## Complete Workflow Example

Here's a typical day-to-day workflow:

### Morning: Extract Latest Metadata
```bash
# 1. Extract metadata with change tracking (enabled by default)
python scripts/extract_metadata.py --all

# Console shows any schema changes detected
# Encrypted files are updated
# Change logs are updated
```

### During Development: View Specific Table
```bash
# 2. Need to debug a specific table
python scripts/decrypt_metadata.py --table FINANCIAL_DATA

# 3. View the decrypted files
cat metadata/decrypted/schemas/FINANCIAL_DATA_metadata.json
cat metadata/decrypted/ddl/FINANCIAL_DATA_create.sql

# 4. Check change history
cat metadata/decrypted/changes/FINANCIAL_DATA_changes.log

# 5. Clean up when done
python scripts/decrypt_metadata.py --clean
```

### Weekly: Review All Changes
```bash
# 1. View summary of all tables with changes
python scripts/view_change_history.py --summary

# 2. Review specific tables that changed
python scripts/view_change_history.py --table FINANCIAL_DATA

# 3. Review changes in last week
python scripts/view_change_history.py --table FINANCIAL_DATA \
  --from 2024-01-01 --to 2024-01-07
```

---

## Command Reference

### Extract Metadata
```bash
# All tables (obfuscation & change tracking enabled by default)
python scripts/extract_metadata.py --all

# All tables, plain text, with change tracking
python scripts/extract_metadata.py --all --no-obfuscate

# Specific table
python scripts/extract_metadata.py --table financial_data

# Force re-extraction even if no changes
python scripts/extract_metadata.py --all --force

# Disable change tracking (not recommended)
python scripts/extract_metadata.py --all --no-check-changes
```

### Decrypt Metadata
```bash
# Decrypt all tables (uses .env password)
python scripts/decrypt_metadata.py --all

# Decrypt specific table
python scripts/decrypt_metadata.py --table FINANCIAL_DATA

# Decrypt with explicit password
python scripts/decrypt_metadata.py --all --password mypassword

# List available tables
python scripts/decrypt_metadata.py --list

# Clean up decrypted files
python scripts/decrypt_metadata.py --clean
```

### View Change History
```bash
# View all changes for a table (obfuscation enabled by default)
python scripts/view_change_history.py --table FINANCIAL_DATA

# View all changes for a table (plain text)
python scripts/view_change_history.py --table FINANCIAL_DATA --no-obfuscate

# View last N changes
python scripts/view_change_history.py --table FINANCIAL_DATA --limit 5

# View changes in date range
python scripts/view_change_history.py --table FINANCIAL_DATA \
  --from 2024-01-01 --to 2024-01-31

# View summary of all tables
python scripts/view_change_history.py --summary

# With explicit password
python scripts/view_change_history.py --table FINANCIAL_DATA --password mypassword
```

---

## Troubleshooting

### Password Issues
```bash
# If password prompt appears but you have .env configured:
# 1. Check .env file has ENCRYPTION_PASSWORD set
# 2. Verify .env is in the project root
# 3. Try explicit password:
python scripts/decrypt_metadata.py --all --password your_password
```

### Cannot Decrypt Files
```bash
# 1. Verify master index exists
ls -la metadata/index.enc

# 2. List available tables
python scripts/decrypt_metadata.py --list

# 3. Check if table name is correct (case-sensitive)
```

### Change Logs Not Created
```bash
# Change tracking is enabled by default
python scripts/extract_metadata.py --all
#                                   (change tracking enabled automatically)
```

### Decrypted Files Committed to Git
```bash
# Clean up
python scripts/decrypt_metadata.py --clean

# Remove from Git
git rm -r --cached metadata/decrypted/

# Verify .gitignore
grep "metadata/decrypted" .gitignore
```

---

## Best Practices

1. **Change tracking is enabled by default** - metadata changes are automatically tracked and logged

2. **Keep encrypted files in Git**, exclude decrypted files:
   - ✅ Commit: `metadata/schemas/*.enc`, `metadata/ddl/*.enc`, `metadata/changes/*.enc`
   - ❌ Never commit: `metadata/decrypted/*`

3. **Clean up decrypted files** after viewing:
   ```bash
   python scripts/decrypt_metadata.py --clean
   ```

4. **Use .env for passwords** instead of command-line arguments:
   ```bash
   # .env
   ENCRYPTION_PASSWORD=your_strong_password
   ```

5. **Review change history regularly** to catch unexpected schema changes:
   ```bash
   python scripts/view_change_history.py --summary
   ```

6. **Archive old encrypted files** are kept automatically with timestamps for rollback

---

## Integration with Data Pipeline

The metadata workflow integrates with your data export/import pipeline:

```bash
# 1. Extract metadata (morning) - obfuscation & change tracking enabled by default
python scripts/extract_metadata.py --all

# 2. Review any schema changes
python scripts/view_change_history.py --summary

# 3. Export data (uses same encryption password)
python scripts/export_data.py --all

# 4. Transfer data to external system
# ... manual transfer or automated process ...

# 5. Import data (uses same encryption password)
python scripts/import_data.py --all
```

All scripts use the same `ENCRYPTION_PASSWORD` from `.env` for consistency!
