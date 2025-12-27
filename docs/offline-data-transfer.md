# Offline Data Transfer Guide

Complete guide for securely transferring large datasets from Snowflake to PostgreSQL using encrypted offline transfer.

## Overview

This process allows you to:
1. Extract data from Snowflake (VPN side)
2. Compress and encrypt data locally
3. Manually transfer encrypted files
4. Decrypt and load into PostgreSQL (external side)

## Prerequisites

- Python 3.11+ with all dependencies installed
- Access to Snowflake (VPN connection)
- Access to PostgreSQL server
- Encryption password (only you know this!)

## Phase 1: Export Data (Snowflake Side)

### 1. Configure Export Settings

Edit `.env` file:
```bash
# Snowflake connection (already configured)
SNOWFLAKE_USER=PETER.LI@CI.COM
SNOWFLAKE_ACCOUNT=CIX-CIX
# ... other Snowflake settings

# Export directory (where to save encrypted files)
EXPORT_BASE_DIR=D:/snowflake_exports

# Compression and chunking
COMPRESSION_TYPE=zstd
COMPRESSION_LEVEL=3
CHUNK_SIZE=100000
```

### 2. Extract Metadata (First Time Only)

```bash
# Extract table schemas
python scripts/extract_metadata.py --all
```

This creates:
- `metadata/schemas/*.json` - Table metadata
- `metadata/ddl/*.sql` - PostgreSQL CREATE TABLE scripts

### 3. Export Table Data

**Single table:**
```bash
python scripts/export_data.py --table financial_data
```

**All tables:**
```bash
python scripts/export_data.py --all
```

**With password file:**
```bash
# Save password to file (secure it!)
echo "your_secure_password" > ~/.encryption_key
chmod 600 ~/.encryption_key

# Use password file
python scripts/export_data.py --table financial_data --password-file ~/.encryption_key
```

### 4. Output

Export creates:
```
D:/snowflake_exports/
└── financial_data/
    ├── data_chunk_001.parquet.enc  (encrypted)
    ├── data_chunk_002.parquet.enc  (encrypted)
    ├── data_chunk_003.parquet.enc  (encrypted)
    └── manifest.json               (metadata)
```

**Manifest contains:**
- Row counts
- Checksums
- Encryption info (salt, algorithm)
- Chunk information

## Phase 2: Manual Transfer

Copy the export directory to PostgreSQL server:

**Option A: USB Drive**
```bash
# Copy to USB
xcopy D:\snowflake_exports E:\USB_DRIVE\ /E /I

# On PostgreSQL server
xcopy E:\USB_DRIVE E:\postgres_imports\ /E /I
```

**Option B: Network Share**
```bash
# Copy to network share
robocopy D:\snowflake_exports \\server\share\imports /E
```

**Option C: SCP (if available)**
```bash
scp -r D:/snowflake_exports user@postgres-server:/path/to/imports
```

## Phase 3: Import Data (PostgreSQL Side)

### 1. Configure Import Settings

Edit `.env` file on PostgreSQL server:
```bash
# PostgreSQL connection (already configured)
POSTGRES_HOST=localhost
POSTGRES_PORT=50211
POSTGRES_USER=postgres
POSTGRES_PASSWORD=$$Pmar1992

# Import directory (where encrypted files are)
IMPORT_BASE_DIR=E:/postgres_imports
```

### 2. Create Tables (First Time Only)

```bash
# Create table structure
psql -h localhost -p 50211 -U postgres -d postgres -f metadata/ddl/financial_data_create.sql
```

### 3. Import Table Data

**Single table:**
```bash
python scripts/import_data.py --table financial_data
```

**All tables:**
```bash
python scripts/import_data.py --all
```

**With password file:**
```bash
python scripts/import_data.py --table financial_data --password-file ~/.encryption_key
```

**Truncate before loading:**
```bash
python scripts/import_data.py --table financial_data --truncate
```

### 4. Verification

The import script automatically:
- ✅ Verifies checksums
- ✅ Decrypts files
- ✅ Loads data
- ✅ Verifies row counts
- ✅ Cleans up temporary files
- ✅ Keeps encrypted files as backup

## Security Features

### Password-Based Encryption

- **Algorithm**: AES-256-GCM
- **Key Derivation**: PBKDF2-HMAC-SHA256
- **Iterations**: 100,000 (configurable)
- **Salt**: Random, stored with file
- **Password**: Never stored anywhere

### What's Encrypted

✅ All data files (.parquet.enc)  
✅ Includes authentication tag (GCM mode)  
❌ Manifest file (contains no sensitive data)

### What's NOT Stored

❌ Your password  
❌ Encryption key  
❌ Any way to decrypt without password

## File Sizes

Typical compression ratios:

| Format | Size (1M rows) | Ratio |
|--------|----------------|-------|
| CSV | ~500 MB | 1x |
| Parquet (snappy) | ~50 MB | 10x |
| Parquet (zstd) | ~25 MB | 20x |
| Parquet (zstd) + Encrypted | ~26 MB | 19x |

## Troubleshooting

### Export Issues

**"Failed to connect to Snowflake"**
- Check VPN connection
- Verify Snowflake credentials in `.env`
- Run `python test_snowflake.py` to test connection

**"Permission denied" on export directory**
- Check `EXPORT_BASE_DIR` path exists
- Verify write permissions

### Import Issues

**"Wrong password or corrupted file"**
- Verify you're using the same password as export
- Check file wasn't corrupted during transfer
- Verify checksums in manifest

**"Table does not exist"**
- Run DDL script first: `psql ... -f metadata/ddl/table_name_create.sql`

**"Row count mismatch"**
- Check PostgreSQL logs for errors
- Verify all chunks were transferred
- Check disk space

## Best Practices

### Security

1. **Use strong passwords** (16+ characters, mixed case, numbers, symbols)
2. **Store password securely** (password manager or secure file)
3. **Don't commit password files** to git
4. **Delete decrypted files** after import (automatic)
5. **Keep encrypted files** as backup

### Performance

1. **Adjust chunk size** based on table size:
   - Small tables (<1M rows): 100,000 rows/chunk
   - Medium tables (1-10M rows): 500,000 rows/chunk
   - Large tables (>10M rows): 1,000,000 rows/chunk

2. **Use zstd compression** for best ratio
3. **Transfer during off-hours** for large datasets

### Verification

1. **Always check row counts** after import
2. **Verify checksums** (automatic)
3. **Test queries** on imported data
4. **Compare sample data** with source

## Example Workflow

```bash
# === SNOWFLAKE SIDE ===

# 1. Extract metadata
python scripts/extract_metadata.py --all

# 2. Export data
python scripts/export_data.py --table financial_data
# Enter password: ********

# 3. Copy to USB/network
# (manual step)

# === POSTGRESQL SIDE ===

# 4. Create table
psql -h localhost -p 50211 -U postgres -d postgres \
  -f metadata/ddl/financial_data_create.sql

# 5. Import data
python scripts/import_data.py --table financial_data
# Enter password: ********

# 6. Verify
psql -h localhost -p 50211 -U postgres -d postgres \
  -c "SELECT COUNT(*) FROM public.financial_data;"
```

## Next Steps

After initial load is complete:
- Set up incremental sync (future feature)
- Configure API for ongoing updates
- Set up monitoring and alerts
