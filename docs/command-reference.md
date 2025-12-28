# Command Reference Guide

Quick reference for all data pipeline commands.

---

## **Complete 4-Step Workflow**

### **SNOWFLAKE SIDE (VPN)**

#### **Step 1: Extract Metadata**
```bash
# Extract metadata for all tables
python scripts/extract_metadata.py --all

# Extract metadata for specific table
python scripts/extract_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

**Output:**
- `metadata/schemas/{table_name}_metadata.json`
- `metadata/ddl/{table_name}_create.sql`

---

#### **Step 2: Export Data**
```bash
# Export all tables (uses filters from config/tables.yaml)
python scripts/export_data.py --all

# Export specific table
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Export with custom chunk size
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --chunk-size 50000

# Export with password file (no prompt)
python scripts/export_data.py --all --password-file ~/.encryption_key
```

**Prompts:**
- Enter encryption password: ********
- Confirm password: ********

**Output:**
- `D:/snowflake_exports/{table_name}/data_chunk_*.parquet.enc`
- `D:/snowflake_exports/{table_name}/manifest.json`

---

### **MANUAL TRANSFER**

Copy these files from Snowflake server to PostgreSQL server:
```
metadata/                    → metadata/
config/tables.yaml           → config/tables.yaml
D:/snowflake_exports/        → E:/postgres_imports/
```

---

### **POSTGRESQL SIDE (External)**

#### **Step 3: Create Tables**
```bash
# Create all tables from DDL files
python scripts/create_tables.py --all

# Create specific table
python scripts/create_tables.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Drop and recreate tables (WARNING: deletes data)
python scripts/create_tables.py --all --drop-existing
```

**Output:**
- Tables created in PostgreSQL with correct schema

---

#### **Step 4: Import Data**
```bash
# Import all tables
python scripts/import_data.py --all

# Import specific table
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Import with password file (no prompt)
python scripts/import_data.py --all --password-file ~/.encryption_key

# Truncate table before loading
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --truncate

# Keep decrypted files for debugging
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --keep-decrypted
```

**Prompts:**
- Enter decryption password: ********

**Output:**
- Data loaded into PostgreSQL tables

---

## **Quick Copy-Paste Commands**

### **For Your Current Table (FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND)**

**Snowflake Side:**
```bash
# Step 1: Extract metadata
python scripts/extract_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Step 2: Export data
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

**PostgreSQL Side:**
```bash
# Step 3: Create table
python scripts/create_tables.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Step 4: Import data
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

---

### **For All Tables**

**Snowflake Side:**
```bash
# Step 1: Extract metadata
python scripts/extract_metadata.py --all

# Step 2: Export data
python scripts/export_data.py --all
```

**PostgreSQL Side:**
```bash
# Step 3: Create tables
python scripts/create_tables.py --all

# Step 4: Import data
python scripts/import_data.py --all
```

---

## **Testing & Verification Commands**

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
psql -h localhost -p 50211 -U postgres -d postgres -c "\dt ms.*"

# Check table structure
psql -h localhost -p 50211 -U postgres -d postgres -c "\d ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"

# Check row count
psql -h localhost -p 50211 -U postgres -d postgres -c "SELECT COUNT(*) FROM ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND;"

# View sample data
psql -h localhost -p 50211 -U postgres -d postgres -c "SELECT * FROM ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND LIMIT 10;"
```

### **File Verification**
```bash
# Check metadata files exist
ls metadata/schemas/
ls metadata/ddl/

# Check export files exist
ls D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/

# Check import files exist
ls E:/postgres_imports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/

# View manifest
cat D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/manifest.json
```

---

## **Common Options**

### **extract_metadata.py**
```bash
--all                    # Extract metadata for all tables in config
--table <name>           # Extract metadata for specific table
```

### **export_data.py**
```bash
--all                    # Export all tables
--table <name>           # Export specific table
--chunk-size <number>    # Rows per chunk (default: 100000)
--password-file <path>   # Read password from file
```

### **create_tables.py**
```bash
--all                    # Create all tables
--table <name>           # Create specific table
--drop-existing          # Drop tables if they exist (recreate)
```

### **import_data.py**
```bash
--all                    # Import all tables
--table <name>           # Import specific table
--password-file <path>   # Read password from file
--truncate               # Truncate table before loading
--keep-decrypted         # Keep decrypted files (for debugging)
```

---

## **Troubleshooting Commands**

### **Check Logs**
```bash
# View pipeline log
cat pipeline.log

# View last 50 lines
tail -n 50 pipeline.log

# Follow log in real-time
tail -f pipeline.log
```

### **Check Manifest**
```bash
# View export manifest
cat D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/manifest.json

# Check filter used
cat D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/manifest.json | grep filter

# Check row count
cat D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/manifest.json | grep total_rows
```

### **Verify Encryption**
```bash
# Check encrypted file size
ls -lh D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/*.enc

# Count encrypted chunks
ls D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/*.enc | wc -l
```

---

## **Environment Setup**

### **Activate Conda Environment**
```bash
# Activate environment
conda activate data-pipeline-system

# Verify Python version
python --version

# Verify packages installed
conda list | grep snowflake
conda list | grep psycopg2
```

### **Check Configuration**
```bash
# Verify .env file exists
cat .env

# Check tables configuration
cat config/tables.yaml
```

---

## **Complete Example Session**

### **Snowflake Server (VPN):**
```bash
# Activate environment
conda activate data-pipeline-system

# Step 1: Extract metadata
python scripts/extract_metadata.py --all
# ✅ Metadata extracted to metadata/

# Step 2: Export data
python scripts/export_data.py --all
# Enter password: MySecurePassword123
# Confirm password: MySecurePassword123
# ✅ Data exported to D:/snowflake_exports/

# Verify exports
ls D:/snowflake_exports/
```

### **Manual Transfer:**
```bash
# Copy to USB drive or network share
# - metadata/
# - config/tables.yaml
# - D:/snowflake_exports/ → E:/postgres_imports/
```

### **PostgreSQL Server (External):**
```bash
# Activate environment
conda activate data-pipeline-system

# Step 3: Create tables
python scripts/create_tables.py --all
# ✅ Tables created in PostgreSQL

# Verify tables
psql -h localhost -p 50211 -U postgres -d postgres -c "\dt ms.*"

# Step 4: Import data
python scripts/import_data.py --all
# Enter password: MySecurePassword123
# ✅ Data imported to PostgreSQL

# Verify data
psql -h localhost -p 50211 -U postgres -d postgres -c "SELECT COUNT(*) FROM ms.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND;"
```

---

## **Quick Reference Table**

| Step | Command | Location | Output |
|------|---------|----------|--------|
| 1. Extract Metadata | `python scripts/extract_metadata.py --all` | Snowflake (VPN) | `metadata/` |
| 2. Export Data | `python scripts/export_data.py --all` | Snowflake (VPN) | `D:/snowflake_exports/` |
| 3. Create Tables | `python scripts/create_tables.py --all` | PostgreSQL | Tables in DB |
| 4. Import Data | `python scripts/import_data.py --all` | PostgreSQL | Data in tables |

---

## **Password Management**

### **Option 1: Interactive (Recommended for first time)**
```bash
# You'll be prompted for password
python scripts/export_data.py --all
# Enter password: ********
# Confirm password: ********
```

### **Option 2: Password File (For automation)**
```bash
# Create password file (one-time)
echo "MySecurePassword123" > ~/.encryption_key
chmod 600 ~/.encryption_key

# Use password file
python scripts/export_data.py --all --password-file ~/.encryption_key
python scripts/import_data.py --all --password-file ~/.encryption_key
```

**⚠️ Security Note:** Keep your password secure! It's not stored anywhere in the system.

---

## **Summary**

**Snowflake Side (2 commands):**
1. `python scripts/extract_metadata.py --all`
2. `python scripts/export_data.py --all`

**PostgreSQL Side (2 commands):**
3. `python scripts/create_tables.py --all`
4. `python scripts/import_data.py --all`

**That's it!** 🎉
