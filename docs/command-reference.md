# Command Reference Guide

Quick reference for all data pipeline commands with obfuscation and change tracking.

---

## **Complete Workflow Overview**

### **SNOWFLAKE SIDE (VPN-connected machine)**

1. **Extract Metadata** - Extract schemas, DDL, track changes
2. **Export Data** - Export actual table data with encryption
3. **View Changes** (optional) - Review metadata changes
4. **Decrypt Metadata** (optional) - Human-readable review

### **MANUAL TRANSFER**

Copy encrypted files to PostgreSQL server

### **POSTGRESQL SIDE (External machine)**

4. **Create Tables** - Create PostgreSQL tables from metadata (one-time)
5. **Import Data** - Decrypt and load into PostgreSQL

---

## **Snowflake Side Commands**

### **1. Extract Metadata** (Schemas, DDL, Change Tracking)

```bash
# Extract metadata for all tables (obfuscation & change tracking enabled by default)
python scripts/extract_metadata.py --all

# Extract metadata for specific table
python scripts/extract_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Force re-extraction even if no changes detected
python scripts/extract_metadata.py --all --force

# Disable obfuscation (not recommended)
python scripts/extract_metadata.py --all --no-obfuscate

# Disable change tracking
python scripts/extract_metadata.py --all --no-check-changes
```

**Features:**
- ✅ **Obfuscation enabled by default** - File names are hashed for security
- ✅ **Change tracking enabled by default** - Detects schema changes automatically
- ✅ **Timestamped archives** - Old versions saved as `table_metadata_20250105.json`
- ✅ **In-memory comparison** - No temporary files written to disk
- ✅ **Deterministic IDs** - Same table = same file ID across runs

**Options:**
- `--all` - Extract metadata for all configured tables
- `--table <name>` - Extract metadata for specific table
- `--force` - Force re-extraction even if no changes detected
- `--no-obfuscate` - Disable name obfuscation
- `--no-check-changes` - Disable change detection
- `--password-file <path>` - Read password from file (for automation)

**Output (Obfuscated):**
- `metadata/schemas/1c81276c664e938a.enc` - Current metadata (encrypted)
- `metadata/ddl/220c0c97431be221.enc` - Current DDL (encrypted)
- `metadata/schemas/a1b2c3d4e5f6g7h8.enc` - Archived metadata (if changed)
- `metadata/ddl/x9y8z7w6v5u4t3s2.enc` - Archived DDL (if changed)
- `metadata/changes/2347fbb2bdc88f34.enc` - Change log (encrypted)

**When decrypted, files restore to:**
- `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata.json` - Current
- `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create.sql` - Current
- `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata_20250105.json` - Archived
- `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create_20250105.sql` - Archived

---

### **2. Export Data** (Actual Table Data)

```bash
# Export all tables (obfuscation enabled by default)
python scripts/export_data.py --all

# Export specific table
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Export with custom chunk size (default: 100,000 rows)
python scripts/export_data.py --all --chunk-size 50000

# Clean existing export before starting (useful for re-runs)
python scripts/export_data.py --all --clean

# Disable obfuscation (not recommended)
python scripts/export_data.py --all --no-obfuscate

# Use password file (for automation)
python scripts/export_data.py --all --password-file ~/.encryption_key
```

**Features:**
- ✅ **Deterministic chunk IDs** - Same table + chunk = same file ID
- ✅ **No data hashing for IDs** - Better performance
- ✅ **Change detection** - Skip unchanged files automatically
- ✅ **Obfuscated folders** - Folder names are hashed
- ✅ **Encrypted manifest** - Metadata about chunks

**Options:**
- `--all` - Export all tables
- `--table <name>` - Export specific table
- `--chunk-size <number>` - Rows per chunk (default: 100,000)
- `--no-obfuscate` - Disable name obfuscation
- `--clean` - Delete existing export folder before starting
- `--password-file <path>` - Read password from file

**Output (Obfuscated):**
```
exports/
└── 1c81276c664e938a/              ← Folder ID (deterministic from table name)
    ├── a1b2c3d4e5f6g7h8.enc       ← Chunk 1 (deterministic from table:chunk:1)
    ├── x9y8z7w6v5u4t3s2.enc       ← Chunk 2 (deterministic from table:chunk:2)
    └── 220c0c97431be221.enc       ← Manifest (deterministic from table name)
```

**Password Prompt:**
```
Enter encryption password: ********
Confirm password: ********
```

---

### **3. View Change History** (Optional - Human Review)

```bash
# View changes for specific table (obfuscation auto-detected from .env)
python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# View last 10 changes
python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --limit 10

# View changes in date range
python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --since 2025-01-01

# Disable obfuscation
python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --no-obfuscate
```

**Features:**
- ✅ **Automatic obfuscation detection** - Reads from `.env`
- ✅ **Formatted output** - Human-readable change descriptions
- ✅ **Date filtering** - View changes in specific time ranges

**Options:**
- `--table <name>` - Table name to view changes for
- `--limit <number>` - Maximum number of entries to show
- `--since <date>` - Show changes since date (YYYY-MM-DD)
- `--no-obfuscate` - Disable obfuscation
- `--password-file <path>` - Read password from file

---

### **4. Decrypt Metadata** (Optional - Human Review)

```bash
# Decrypt all tables to metadata/decrypted/ (git-ignored)
python scripts/decrypt_metadata.py --all

# Decrypt specific table
python scripts/decrypt_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# List available tables (reads from config/tables.yaml)
python scripts/decrypt_metadata.py --list

# Clean up decrypted files
python scripts/decrypt_metadata.py --clean

# Show change history after decrypting
python scripts/decrypt_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --show-changes
```

**Features:**
- ✅ **No index.enc needed** - Reads table names from `config/tables.yaml`
- ✅ **Deterministic file discovery** - Generates IDs from table names
- ✅ **Restores timestamped filenames** - Archives show dates
- ✅ **Git-ignored output** - Decrypted files not tracked

**Options:**
- `--all` - Decrypt all tables
- `--table <name>` - Decrypt specific table
- `--list` - List available tables
- `--clean` - Delete all decrypted files
- `--show-changes` - Display change history after decrypting
- `--password-file <path>` - Read password from file
- `--output-dir <path>` - Custom output directory (default: metadata/decrypted)

**Output:**
```
metadata/decrypted/
├── schemas/
│   ├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata.json          ← Current
│   ├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata_20250105.json ← Archived
│   └── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata_20250102.json ← Archived
├── ddl/
│   ├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create.sql             ← Current
│   ├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create_20250105.sql    ← Archived
│   └── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create_20250102.sql    ← Archived
└── changes/
    └── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_changes.log            ← Change log
```

---

## **Manual Transfer**

Copy these files from Snowflake server to PostgreSQL server:

```bash
# Copy metadata folder
scp -r metadata/ user@psql-server:/path/to/project/

# Copy exports folder
scp -r exports/ user@psql-server:/path/to/project/

# Or use USB drive, network share, etc.
```

**What to transfer:**
- `metadata/` - All encrypted metadata files
- `exports/` - All encrypted data files
- `config/tables.yaml` - Table configuration (if not already on PostgreSQL server)

---

## **PostgreSQL Side Commands**

### **5. Create PostgreSQL Tables** (One-time Setup)

```bash
# Create all tables from DDL files (requires metadata/ folder transferred)
python scripts/create_tables.py --all

# Create specific table
python scripts/create_tables.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Drop and recreate tables (useful for schema changes)
python scripts/create_tables.py --all --drop-existing

# Use password file (for encrypted DDL files)
python scripts/create_tables.py --all --password-file ~/.encryption_key
```

**Features:**
- ✅ **Supports encrypted DDL files** - Automatically detects and decrypts
- ✅ **Deterministic file discovery** - Finds encrypted files using table names
- ✅ **Schema verification** - Compares created table with metadata
- ✅ **Automatic DDL execution** - Creates tables with correct structure
- ✅ **Drop option** - Recreate tables when schema changes

**Options:**
- `--all` - Create all configured tables
- `--table <name>` - Create specific table
- `--drop-existing` - Drop table before creating (use with caution!)
- `--password-file <path>` - Read password from file (for encrypted DDL)

**When to use:**
- ✅ First time setup (before importing data)
- ✅ After metadata changes detected
- ✅ When table structure needs to be updated

**Note:** This command reads the encrypted DDL files from `metadata/ddl/`, so you need to have the `metadata/` folder transferred from Snowflake server first. Password is auto-sourced from `.env` if files are encrypted.

---

### **6. Import Data** (Decrypt and Load)

```bash
# Import all tables
python scripts/import_data.py --all

# Import specific table
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Truncate table before loading
python scripts/import_data.py --all --truncate

# Keep decrypted Parquet files (for debugging)
python scripts/import_data.py --all --keep-decrypted

# Use password file (for automation)
python scripts/import_data.py --all --password-file ~/.encryption_key
```

**Features:**
- ✅ **Automatic folder discovery** - Finds obfuscated folders using deterministic IDs
- ✅ **Manifest-based import** - Reads chunk information from manifest
- ✅ **Checksum verification** - Ensures data integrity
- ✅ **Row count verification** - Confirms all data loaded

**Options:**
- `--all` - Import all tables
- `--table <name>` - Import specific table
- `--truncate` - Truncate table before loading
- `--keep-decrypted` - Keep decrypted Parquet files (for debugging)
- `--password-file <path>` - Read password from file

**Password Prompt:**
```
Enter decryption password: ********
```

---

## **Quick Copy-Paste Commands**

### **For Specific Table**

**Snowflake Side:**
```bash
# Extract metadata
python scripts/extract_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Export data
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

**PostgreSQL Side:**
```bash
# Create tables (one-time)
python scripts/create_tables.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Import data
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

---

### **For All Tables**

**Snowflake Side:**
```bash
# Extract metadata
python scripts/extract_metadata.py --all

# Export data
python scripts/export_data.py --all
```

**PostgreSQL Side:**
```bash
# Create tables (one-time)
python scripts/create_tables.py --all

# Import data
python scripts/import_data.py --all
```

---

## **Password Management**

### **Option 1: Environment Variable (Recommended)**

Set in `.env` file:
```bash
ENCRYPTION_PASSWORD=your_secure_password_here
OBFUSCATE_NAMES=true
```

Scripts automatically use this password.

### **Option 2: Password File (For Automation)**

```bash
# Create password file (one-time)
echo "MySecurePassword123" > ~/.encryption_key
chmod 600 ~/.encryption_key

# Use in commands
python scripts/export_data.py --all --password-file ~/.encryption_key
python scripts/import_data.py --all --password-file ~/.encryption_key
```

### **Option 3: Interactive Prompt (Most Secure)**

```bash
# You'll be prompted for password
python scripts/export_data.py --all
# Enter password: ********
# Confirm password: ********
```

**⚠️ Security Note:** Keep your password secure! It's not stored anywhere in the system.

---

## **Configuration Files**

### **config/tables.yaml**

Defines which tables to sync:

```yaml
tables:
  - name: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    snowflake:
      database: "CIGAM_PRD_RL"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
      filter:
        - "WHERE _ID IN (SELECT mstarid FROM ...)"
    postgres:
      schema: "ms"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

### **.env**

Environment configuration:

```bash
# Encryption
ENCRYPTION_PASSWORD=your_secure_password_here
OBFUSCATE_NAMES=true

# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_AUTH_METHOD=sso
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse

# PostgreSQL Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DATABASE=postgres

# Export/Import Directories
EXPORT_BASE_DIR=exports
IMPORT_BASE_DIR=imports
```

---

## **Testing & Verification**

### **Connection Tests**

```bash
# Test Snowflake connection (VPN required)
python test_snowflake.py

# Test PostgreSQL connection
python test_postgres.py
```

### **PostgreSQL Verification**

```bash
# List all tables
psql -h localhost -p 5432 -U postgres -d postgres -c "\dt ms.*"

# Check table structure
psql -h localhost -p 5432 -U postgres -d postgres -c "\d ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"

# Check row count
psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT COUNT(*) FROM ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND;"

# View sample data
psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT * FROM ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND LIMIT 10;"
```

### **File Verification**

```bash
# Check metadata files exist
ls metadata/schemas/
ls metadata/ddl/

# Check export files exist
ls exports/

# List obfuscated folders
ls exports/

# View manifest (need to decrypt first)
python scripts/decrypt_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

---

## **Troubleshooting**

### **Common Issues**

**1. "Master index not found" error**
- **Solution:** This is expected! We don't use index.enc anymore. The script reads from `config/tables.yaml` instead.

**2. "Wrong password" error**
- **Solution:** Ensure password matches what was used during export. Check `.env` file.

**3. "Table not found in config/tables.yaml"**
- **Solution:** Add the table to `config/tables.yaml` first.

**4. "Encrypted file not found"**
- **Solution:** Ensure files were transferred from Snowflake server. Check folder names (they're obfuscated).

**5. Change detection not working**
- **Solution:** Ensure `--no-check-changes` is NOT used. Change tracking is enabled by default.

### **Check Logs**

```bash
# View pipeline log
cat pipeline.log

# View last 50 lines
tail -n 50 pipeline.log

# Follow log in real-time
tail -f pipeline.log
```

---

## **Key Features Summary**

### **Obfuscation (Enabled by Default)**
- ✅ Folder names hashed: `1c81276c664e938a/`
- ✅ File names hashed: `a1b2c3d4e5f6g7h8.enc`
- ✅ Deterministic IDs: Same table = same ID
- ✅ No index file needed: IDs generated from table names
- ✅ Content encrypted: AES-256-GCM

### **Change Tracking (Enabled by Default)**
- ✅ Automatic detection: Compares old vs new metadata
- ✅ Timestamped archives: `table_metadata_20250105.json`
- ✅ Change logs: Detailed history of all changes
- ✅ In-memory comparison: No temp files on disk
- ✅ Alerts: Console warnings when changes detected

### **Performance Optimizations**
- ✅ Deterministic chunk IDs: No data hashing needed
- ✅ Change detection: Skip unchanged files
- ✅ In-memory operations: Faster comparisons
- ✅ Efficient encryption: AES-256-GCM with PBKDF2

---

## **Complete Example Session**

### **Snowflake Server (VPN):**

```bash
# Activate environment
conda activate ms-pipeline

# Step 1: Extract metadata (with change tracking)
python scripts/extract_metadata.py --all
# ✅ Metadata extracted to metadata/
# ⚠️  METADATA CHANGES DETECTED! (if any)

# Step 2: Export data
python scripts/export_data.py --all
# Enter password: MySecurePassword123
# Confirm password: MySecurePassword123
# ✅ Data exported to exports/

# Optional: View changes
python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Optional: Decrypt for review
python scripts/decrypt_metadata.py --all
# ✅ Decrypted to metadata/decrypted/
```

### **Manual Transfer:**

```bash
# Copy to USB drive or network share
# - metadata/
# - exports/
```

### **PostgreSQL Server (External):**

```bash
# Activate environment
conda activate ms-pipeline

# Step 3: Create tables (one-time setup)
python scripts/create_tables.py --all
# ✅ Tables created in PostgreSQL

# Step 4: Import data
python scripts/import_data.py --all
# Enter password: MySecurePassword123
# ✅ Data imported to PostgreSQL

# Verify data
psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT COUNT(*) FROM ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND;"
```

---

## **Quick Reference Table**

| Step | Command | Location | Output |
|------|---------|----------|--------|
| 1. Extract Metadata | `python scripts/extract_metadata.py --all` | Snowflake (VPN) | `metadata/` (encrypted) |
| 2. Export Data | `python scripts/export_data.py --all` | Snowflake (VPN) | `exports/` (encrypted) |
| 3. Transfer | Manual copy | Both | Files moved |
| 4. Create Tables | `python scripts/create_tables.py --all` | PostgreSQL | Tables created |
| 5. Import Data | `python scripts/import_data.py --all` | PostgreSQL | Data in tables |

---

## **Summary**

**Snowflake Side (2 commands):**
```bash
python scripts/extract_metadata.py --all
python scripts/export_data.py --all
```

**PostgreSQL Side (2 commands):**
```bash
python scripts/create_tables.py --all
python scripts/import_data.py --all
```

**That's it!** 🎉

All encryption, obfuscation, change tracking, and version control happen automatically.
