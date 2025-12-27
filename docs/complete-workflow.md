# Complete Data Transfer Workflow

End-to-end guide for transferring data from Snowflake to PostgreSQL across network boundaries.

---

## **Overview**

```
┌─────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE SIDE (VPN)                      │
└─────────────────────────────────────────────────────────────┘
    │
    │ 1. Extract Metadata
    │    python scripts/extract_metadata.py --all
    │    → metadata/schemas/*.json
    │    → metadata/ddl/*.sql
    │
    │ 2. Export Data
    │    python scripts/export_data.py --all
    │    → D:/snowflake_exports/table_name/*.parquet.enc
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│                    MANUAL TRANSFER                           │
│  Copy files from Snowflake server to PostgreSQL server      │
│  - metadata/ folder                                          │
│  - config/tables.yaml                                        │
│  - D:/snowflake_exports/ → E:/postgres_imports/             │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│                  POSTGRESQL SIDE (External)                  │
└─────────────────────────────────────────────────────────────┘
    │
    │ 3. Create Tables
    │    python scripts/create_tables.py --all
    │    → Creates tables in PostgreSQL
    │
    │ 4. Import Data
    │    python scripts/import_data.py --all
    │    → Loads data from encrypted files
    │
    ▼
   DONE!
```

---

## **Phase 1: Metadata (Snowflake Side)**

### **Step 1.1: Extract Metadata**

```bash
# Extract table schemas and generate DDL
python scripts/extract_metadata.py --all
```

**Output:**
```
metadata/
├── schemas/
│   ├── financial_data_metadata.json
│   └── market_prices_metadata.json
└── ddl/
    ├── financial_data_create.sql
    └── market_prices_create.sql
```

**What this does:**
- ✅ Connects to Snowflake
- ✅ Extracts table schemas
- ✅ Maps Snowflake types to PostgreSQL types
- ✅ Generates CREATE TABLE DDL scripts
- ✅ Saves metadata as JSON

---

## **Phase 2: Data Export (Snowflake Side)**

### **Step 2.1: Export Data**

```bash
# Export all tables
python scripts/export_data.py --all

# Or export specific table
python scripts/export_data.py --table financial_data
```

**Prompts:**
```
Enter encryption password: ********
Confirm password: ********
```

**Output:**
```
D:/snowflake_exports/
├── financial_data/
│   ├── data_chunk_001.parquet.enc
│   ├── data_chunk_002.parquet.enc
│   ├── data_chunk_003.parquet.enc
│   └── manifest.json
└── market_prices/
    ├── data_chunk_001.parquet.enc
    └── manifest.json
```

**What this does:**
- ✅ Extracts data from Snowflake in chunks
- ✅ Compresses data (Parquet + zstd)
- ✅ Encrypts files (AES-256-GCM)
- ✅ Generates manifest with checksums
- ✅ Saves to local directory

---

## **Phase 3: Manual Transfer**

### **Step 3.1: Copy Files to PostgreSQL Server**

**Files to transfer:**
```
Source (Snowflake server):
├── metadata/                    → Copy entire folder
├── config/tables.yaml           → Copy file
└── D:/snowflake_exports/        → Copy entire folder

Destination (PostgreSQL server):
├── metadata/                    → Same location
├── config/tables.yaml           → Same location
└── E:/postgres_imports/         → Configured in .env
```

**Transfer methods:**
- USB drive
- Network share
- SCP/SFTP
- Cloud storage (encrypted)

**Verification:**
```bash
# On PostgreSQL server, verify files exist
ls metadata/ddl/
ls metadata/schemas/
ls E:/postgres_imports/financial_data/
```

---

## **Phase 4: Create Tables (PostgreSQL Side)**

### **Step 4.1: Create All Tables**

```bash
# Create all tables from DDL files
python scripts/create_tables.py --all
```

**What this does:**
- ✅ Reads config/tables.yaml
- ✅ Loops through all tables
- ✅ Checks if DDL files exist
- ✅ Creates tables if they don't exist
- ✅ Verifies table structure
- ✅ Shows summary

**Output:**
```
======================================================================
Creating All Tables from config/tables.yaml
======================================================================

📋 Found 2 tables to create:
   - financial_data
   - market_prices

======================================================================
Creating Table: financial_data
======================================================================
✅ Found metadata: financial_data_metadata.json
✅ Found DDL: financial_data_create.sql

🔄 Creating table...
✅ Table created successfully!
   Schema: public
   Table: financial_data
   Columns: 50

🔍 Verifying table structure...
✅ Table structure verified!
   Snowflake columns: 50
   PostgreSQL columns: 50

📊 Table info:
   Rows: 0
   Size: 8192 bytes

======================================================================
TABLE CREATION SUMMARY
======================================================================
Total tables: 2
✅ Successful: 2
❌ Failed: 0

📋 Results:
   ✅ financial_data: success
   ✅ market_prices: success
======================================================================
```

### **Step 4.2: Create Single Table (Optional)**

```bash
# Create specific table
python scripts/create_tables.py --table financial_data
```

### **Step 4.3: Recreate Tables (Optional)**

```bash
# Drop and recreate all tables
python scripts/create_tables.py --all --drop-existing
```

---

## **Phase 5: Import Data (PostgreSQL Side)**

### **Step 5.1: Import All Data**

```bash
# Import all tables
python scripts/import_data.py --all
```

**Prompts:**
```
Enter decryption password: ********
```

**What this does:**
- ✅ Reads manifest files
- ✅ Decrypts data files
- ✅ Verifies checksums
- ✅ Loads data to PostgreSQL
- ✅ Verifies row counts
- ✅ Cleans up temporary files

**Output:**
```
======================================================================
IMPORTING TABLE: financial_data
======================================================================

📋 Manifest loaded:
   Export date: 2024-01-15T10:30:00Z
   Total rows: 1,234,567
   Total chunks: 3

🔄 Processing 3 chunks...

📦 Chunk 1/3:
   File: data_chunk_001.parquet.enc
   Rows: 500,000
   🔓 Decrypting...
   ✅ Verifying checksum...
   📥 Loading to PostgreSQL...
   ✅ Loaded 500,000 rows

📦 Chunk 2/3:
   ...

🔍 Verifying row count...
✅ Row count verified: 1,234,567 rows

🗑️  Cleaning up temporary files...
✅ Removed 3 temporary files

📊 Table information:
   Rows: 1,234,567
   Size: 125 MB

======================================================================
✅ IMPORT COMPLETE!
======================================================================
📊 Total: 1,234,567 rows loaded
📁 Table: public.financial_data
💾 Size: 125 MB
💾 Encrypted files kept as backup in: E:/postgres_imports/financial_data
======================================================================
```

### **Step 5.2: Import Single Table (Optional)**

```bash
# Import specific table
python scripts/import_data.py --table financial_data
```

### **Step 5.3: Truncate Before Import (Optional)**

```bash
# Clear table and reload
python scripts/import_data.py --table financial_data --truncate
```

---

## **Complete Command Reference**

### **Snowflake Side:**
```bash
# 1. Extract metadata
python scripts/extract_metadata.py --all

# 2. Export data
python scripts/export_data.py --all
```

### **Manual Transfer:**
```bash
# Copy files to PostgreSQL server
# (Use your preferred method)
```

### **PostgreSQL Side:**
```bash
# 3. Create tables
python scripts/create_tables.py --all

# 4. Import data
python scripts/import_data.py --all
```

---

## **Verification**

### **After Table Creation:**
```bash
# List tables
psql -h localhost -p 50211 -U postgres -d postgres -c "\dt public.*"

# Check table structure
psql -h localhost -p 50211 -U postgres -d postgres -c "\d public.financial_data"
```

### **After Data Import:**
```bash
# Check row count
psql -h localhost -p 50211 -U postgres -d postgres -c "SELECT COUNT(*) FROM public.financial_data;"

# Check sample data
psql -h localhost -p 50211 -U postgres -d postgres -c "SELECT * FROM public.financial_data LIMIT 10;"
```

---

## **Troubleshooting**

### **Metadata Issues:**
```bash
# If metadata files are missing
python scripts/extract_metadata.py --all

# Verify files exist
ls metadata/ddl/
ls metadata/schemas/
```

### **Table Creation Issues:**
```bash
# Test PostgreSQL connection
python test_postgres.py

# Check if table already exists
psql -h localhost -p 50211 -U postgres -d postgres -c "\dt public.*"

# Drop and recreate
python scripts/create_tables.py --table financial_data --drop-existing
```

### **Data Import Issues:**
```bash
# Verify encrypted files exist
ls E:/postgres_imports/financial_data/

# Check manifest
cat E:/postgres_imports/financial_data/manifest.json

# Test with single table
python scripts/import_data.py --table financial_data
```

---

## **Best Practices**

### **Security:**
1. ✅ Use strong encryption password (16+ characters)
2. ✅ Store password securely (password manager)
3. ✅ Don't commit password to git
4. ✅ Keep encrypted files as backup
5. ✅ Delete decrypted files after import (automatic)

### **Performance:**
1. ✅ Adjust chunk size based on table size
2. ✅ Use zstd compression for best ratio
3. ✅ Transfer during off-hours
4. ✅ Verify checksums (automatic)

### **Workflow:**
1. ✅ Test with one table first
2. ✅ Verify row counts match
3. ✅ Check sample data
4. ✅ Then process all tables
5. ✅ Keep logs for audit trail

---

## **Summary**

| Phase | Location | Command | Output |
|-------|----------|---------|--------|
| 1. Metadata | Snowflake | `extract_metadata.py --all` | metadata/*.json, *.sql |
| 2. Export | Snowflake | `export_data.py --all` | D:/snowflake_exports/*.enc |
| 3. Transfer | Manual | Copy files | E:/postgres_imports/*.enc |
| 4. Create Tables | PostgreSQL | `create_tables.py --all` | Tables in PostgreSQL |
| 5. Import | PostgreSQL | `import_data.py --all` | Data in PostgreSQL |

**Total time:** Depends on data size (typically 1-4 hours for large datasets)
