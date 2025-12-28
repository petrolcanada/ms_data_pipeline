# Name Obfuscation Guide

Complete guide for using name obfuscation to enhance security of exported data.

---

## **Overview**

Name obfuscation replaces table names and file names with random identifiers, making exported data less identifiable to casual observers. This adds an extra layer of security when transferring sensitive data.

### **What Gets Obfuscated**

✅ **Folder names**: `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND` → `a7f3d9e2c4b8f1a9`
✅ **File names**: `data_chunk_001.parquet.enc` → `b4c8f1a9.enc`
✅ **Master index**: Encrypted mapping file (`.export_index.enc`)

### **What Stays the Same**

❌ **Manifest files**: Still named `manifest.json` (contains no sensitive data)
❌ **File contents**: Data encryption unchanged
❌ **Import process**: Automatic detection and handling

---

## **How It Works**

### **Export Process**

```
1. Enable obfuscation (--obfuscate flag or OBFUSCATE_NAMES=true)
2. For each table:
   - Generate random folder ID (16 hex characters)
   - Generate random file IDs for each chunk
   - Create manifest with file mappings
3. Create encrypted master index (.export_index.enc)
   - Maps folder IDs to table names
   - Encrypted with same password as data
```

### **Directory Structure**

**Without Obfuscation:**
```
D:/snowflake_exports/
└── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/
    ├── data_chunk_001.parquet.enc
    ├── data_chunk_002.parquet.enc
    └── manifest.json
```

**With Obfuscation:**
```
D:/snowflake_exports/
├── a7f3d9e2c4b8f1a9/              # Random folder ID
│   ├── b4c8f1a9.enc               # Random file ID
│   ├── e2d5a7c3.enc               # Random file ID
│   └── manifest.json              # Contains mappings
└── .export_index.enc              # Encrypted master index
```

---

## **Usage**

### **Enable Obfuscation**

**Option 1: Command Line Flag (Recommended)**
```bash
# Export with obfuscation
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --obfuscate

# Export all tables with obfuscation
python scripts/export_data.py --all --obfuscate
```

**Option 2: Environment Variable**
```bash
# Add to .env file
OBFUSCATE_NAMES=true

# Then run normally
python scripts/export_data.py --all
```

**Option 3: Disable Explicitly**
```bash
# Override environment variable
python scripts/export_data.py --all --no-obfuscate
```

### **Priority Order**

1. **Command line flags** (`--obfuscate` or `--no-obfuscate`)
2. **Environment variable** (`OBFUSCATE_NAMES`)
3. **Default** (False - backward compatible)

---

## **Export Examples**

### **Single Table with Obfuscation**

```bash
python scripts/export_data.py \
  --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND \
  --obfuscate
```

**Output:**
```
======================================================================
EXPORTING TABLE: FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
🔒 Name obfuscation: ENABLED
======================================================================
📁 Export folder: a7f3d9e2c4b8f1a9 (obfuscated)

🔍 Filter: WHERE _id in (...)

🔄 Estimating table size...
✅ Filtered table size: 50,000 rows (25.5 MB estimated)

🔄 Extracting data in chunks of 100,000 rows...

📦 Processing chunk 1...
   Rows: 50,000
   File: b4c8f1a9.enc (obfuscated)
   Compressed: 12.3 MB
   🔐 Encrypting...
   Encrypted: 12.4 MB

======================================================================
✅ EXPORT COMPLETE!
======================================================================
📁 Location: D:/snowflake_exports/a7f3d9e2c4b8f1a9
🔒 Folder ID: a7f3d9e2c4b8f1a9
📊 Total: 50,000 rows in 1 chunks
🔍 Filter applied: WHERE _id in (...)
💾 Size: 12.4 MB (encrypted)
🔐 Encryption: AES-256-GCM with PBKDF2 (100,000 iterations)
🔒 Names: Obfuscated (see master index)
⚠️  Remember your password - it's not stored anywhere!
======================================================================

======================================================================
CREATING MASTER INDEX
======================================================================
✅ Master index created: D:/snowflake_exports/.export_index.enc
   Tables: 1
   Size: 0.85 KB
   Checksum: a3f7d9e2c4b8f1a9...

⚠️  Keep this file with your exports!
   It maps obfuscated folder names to table names
======================================================================
```

### **Multiple Tables with Obfuscation**

```bash
python scripts/export_data.py --all --obfuscate
```

**Output:**
```
🔒 Name obfuscation: ENABLED
   Folder and file names will be randomized
   Master index will be created: .export_index.enc

🔐 Connecting to Snowflake...
✅ Connected to Snowflake (SSO authentication complete)

======================================================================
EXPORTING 3 TABLES
======================================================================

[... exports each table with obfuscated names ...]

======================================================================
ALL EXPORTS COMPLETE
======================================================================

======================================================================
CREATING MASTER INDEX
======================================================================
✅ Master index created: D:/snowflake_exports/.export_index.enc
   Tables: 3
   Size: 1.2 KB
   Checksum: a3f7d9e2c4b8f1a9...

⚠️  Keep this file with your exports!
   It maps obfuscated folder names to table names
======================================================================
```

---

## **Master Index File**

### **Purpose**

The master index (`.export_index.enc`) is an encrypted file that maps obfuscated folder IDs back to real table names. It's required for importing obfuscated exports.

### **Contents (Before Encryption)**

```json
{
  "version": "1.0",
  "created_at": "2024-01-15T10:30:00Z",
  "obfuscation_enabled": true,
  "tables": [
    {
      "table_name": "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND",
      "folder_id": "a7f3d9e2c4b8f1a9",
      "export_timestamp": "2024-01-15T10:30:00Z",
      "total_rows": 50000,
      "total_chunks": 1
    },
    {
      "table_name": "MARKET_PRICES",
      "folder_id": "e2d5a7c3f9b1d4e8",
      "export_timestamp": "2024-01-15T10:35:00Z",
      "total_rows": 100000,
      "total_chunks": 2
    }
  ]
}
```

### **Security**

- ✅ **Encrypted** with AES-256-GCM (same as data files)
- ✅ **Password-protected** (same password as data)
- ✅ **Checksum verified** during decryption
- ✅ **Required for import** (must transfer with data)

---

## **Manifest File**

Each export folder contains a `manifest.json` file with chunk metadata and file mappings.

### **With Obfuscation**

```json
{
  "table_name": "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND",
  "export_timestamp": "2024-01-15T10:30:00Z",
  "total_rows": 50000,
  "total_chunks": 1,
  "obfuscation_enabled": true,
  "file_mappings": {
    "b4c8f1a9.enc": {
      "chunk_number": 1,
      "rows": 50000
    }
  },
  "snowflake_source": { ... },
  "encryption": { ... },
  "compression": { ... },
  "chunks": [ ... ]
}
```

### **Key Fields**

- **obfuscation_enabled**: Indicates if names are obfuscated
- **file_mappings**: Maps obfuscated file names to chunk numbers
- **table_name**: Real table name (for verification)

---

## **Import Process**

The import script automatically detects and handles obfuscated exports.

### **Automatic Detection**

```bash
# Import works the same way
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

**Process:**
1. Check if `.export_index.enc` exists
2. If yes, decrypt master index with password
3. Find folder ID for requested table
4. Read manifest from obfuscated folder
5. Process chunks in correct order using file mappings
6. Import data normally

### **No Special Flags Needed**

The import script automatically:
- ✅ Detects obfuscated exports
- ✅ Decrypts master index
- ✅ Finds correct folder
- ✅ Processes chunks in order
- ✅ Verifies table names match

---

## **Security Benefits**

### **Protection Against Casual Observation**

**Without Obfuscation:**
```bash
$ ls D:/snowflake_exports/
FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/
MARKET_PRICES/
CUSTOMER_DATA/
```
❌ **Anyone can see table names**

**With Obfuscation:**
```bash
$ ls D:/snowflake_exports/
a7f3d9e2c4b8f1a9/
e2d5a7c3f9b1d4e8/
f1b4c8a9d5e2a7c3/
.export_index.enc
```
✅ **Table names hidden**

### **File Name Protection**

**Without Obfuscation:**
```bash
$ ls D:/snowflake_exports/CUSTOMER_DATA/
data_chunk_001.parquet.enc
data_chunk_002.parquet.enc
data_chunk_003.parquet.enc
```
❌ **Reveals chunk count and structure**

**With Obfuscation:**
```bash
$ ls D:/snowflake_exports/a7f3d9e2c4b8f1a9/
b4c8f1a9.enc
e2d5a7c3.enc
f9b1d4e8.enc
manifest.json
```
✅ **No information revealed**

### **Defense in Depth**

Obfuscation adds an extra layer on top of:
1. **Encryption** (AES-256-GCM)
2. **Password protection** (PBKDF2)
3. **Compression** (zstd)
4. **Checksums** (SHA-256)

---

## **Best Practices**

### **When to Use Obfuscation**

✅ **Use when:**
- Transferring highly sensitive data
- Using USB drives or physical media
- Storing exports in shared locations
- Compliance requires name protection
- Extra security layer desired

❌ **Not needed when:**
- Exports stay on secure servers
- Network transfer is encrypted (VPN/TLS)
- Only authorized personnel have access
- Performance is critical (minimal overhead though)

### **Important Notes**

1. **Keep master index with exports**
   - Required for import
   - Transfer `.export_index.enc` with data folders

2. **Use same password**
   - Master index uses same password as data
   - One password for everything

3. **Backward compatible**
   - Can mix obfuscated and non-obfuscated exports
   - Import script handles both automatically

4. **Performance impact**
   - Minimal (< 1% overhead)
   - Identifier generation is fast
   - No impact on encryption/compression

---

## **Troubleshooting**

### **Master Index Not Found**

**Error:**
```
Error: Master index not found: D:/snowflake_exports/.export_index.enc
```

**Solution:**
- Ensure `.export_index.enc` was transferred with data
- Check file wasn't filtered out (starts with dot)
- Verify file permissions

### **Wrong Password**

**Error:**
```
Failed to decrypt master index. Verify password is correct.
```

**Solution:**
- Use same password as export
- Check for typos
- Verify password file if using `--password-file`

### **Table Not in Index**

**Error:**
```
Table 'CUSTOMER_DATA' not found in master index
```

**Solution:**
- Verify table was exported with obfuscation
- Check table name spelling
- Ensure correct master index file

---

## **Command Reference**

### **Export Commands**

```bash
# Enable obfuscation (command line)
python scripts/export_data.py --table <name> --obfuscate
python scripts/export_data.py --all --obfuscate

# Disable obfuscation (override environment)
python scripts/export_data.py --all --no-obfuscate

# With password file
python scripts/export_data.py --all --obfuscate --password-file ~/.encryption_key
```

### **Import Commands**

```bash
# Import works the same (automatic detection)
python scripts/import_data.py --table <name>
python scripts/import_data.py --all

# With password file
python scripts/import_data.py --all --password-file ~/.encryption_key
```

### **Environment Variable**

```bash
# Add to .env file
OBFUSCATE_NAMES=true

# Or set temporarily
export OBFUSCATE_NAMES=true
python scripts/export_data.py --all
```

---

## **Summary**

| Feature | Without Obfuscation | With Obfuscation |
|---------|---------------------|------------------|
| Folder names | Table names | Random IDs |
| File names | Sequential | Random IDs |
| Master index | Not created | Encrypted file |
| Security | Encryption only | Encryption + Obfuscation |
| Import | Automatic | Automatic |
| Performance | Baseline | +1% overhead |
| Backward compatible | Yes | Yes |

**Key Takeaway:** Obfuscation adds an extra security layer by hiding table and file names, making casual observation reveal nothing about your data structure.

