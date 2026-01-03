# Metadata Management Workflow Diagram

## Complete System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SNOWFLAKE (VPN Side)                               │
│                                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                      │
│  │ FINANCIAL_   │  │ MARKET_      │  │ OTHER_       │                      │
│  │ DATA         │  │ DATA         │  │ TABLES       │                      │
│  └──────────────┘  └──────────────┘  └──────────────┘                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ SSO Authentication
                                    │ VPN Connection
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    METADATA EXTRACTION PIPELINE                              │
│                                                                               │
│  python scripts/extract_metadata.py --all --obfuscate --check-changes       │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 1. Connect to Snowflake                                              │   │
│  │ 2. Query INFORMATION_SCHEMA for table metadata                       │   │
│  │ 3. Extract column definitions, types, constraints                    │   │
│  │ 4. Generate PostgreSQL DDL                                           │   │
│  │ 5. Compare with previous metadata (if exists)                        │   │
│  │ 6. Detect changes (columns added/removed/modified)                   │   │
│  │ 7. Archive old files if changes detected                             │   │
│  │ 8. Encrypt metadata, DDL, and change logs                            │   │
│  │ 9. Save to metadata/ directory                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ENCRYPTED METADATA STORAGE                              │
│                         (Git Repository)                                     │
│                                                                               │
│  metadata/                                                                   │
│  ├── schemas/                                                                │
│  │   ├── 4923cba5118f2c90.enc          ← Current encrypted metadata         │
│  │   ├── 4923cba5118f2c90_20240103.enc ← Archived (old version)            │
│  │   └── 7b371a289b3b1fef.enc                                               │
│  ├── ddl/                                                                    │
│  │   ├── 18a094ce60d6f8ed.enc          ← Current encrypted DDL              │
│  │   ├── 18a094ce60d6f8ed_20240103.enc ← Archived (old version)            │
│  │   └── 3f2c94139c5434a6.enc                                               │
│  ├── changes/                                                                │
│  │   ├── a1b2c3d4e5f6g7h8.enc          ← Encrypted change logs              │
│  │   └── i9j0k1l2m3n4o5p6.enc                                               │
│  └── index.enc                          ← Master index (table name mapping) │
│                                                                               │
│  ✅ Safe to commit to Git                                                   │
│  🔒 Encrypted with AES-256-GCM                                              │
│  📝 Deterministic file IDs (same table = same ID)                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ When needed for debugging/viewing
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DECRYPTION UTILITY                                      │
│                                                                               │
│  python scripts/decrypt_metadata.py --all                                   │
│  python scripts/decrypt_metadata.py --table FINANCIAL_DATA                  │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 1. Read password from .env (ENCRYPTION_PASSWORD)                     │   │
│  │ 2. Decrypt master index (index.enc)                                  │   │
│  │ 3. Find table name → file ID mapping                                 │   │
│  │ 4. Decrypt metadata file (*.enc → *_metadata.json)                   │   │
│  │ 5. Decrypt DDL file (*.enc → *_create.sql)                           │   │
│  │ 6. Decrypt change log (*.enc → *_changes.log)                        │   │
│  │ 7. Save to metadata/decrypted/                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   DECRYPTED FILES (Temporary)                                │
│                      (NOT in Git)                                            │
│                                                                               │
│  metadata/decrypted/                                                         │
│  ├── schemas/                                                                │
│  │   ├── FINANCIAL_DATA_metadata.json  ← Human-readable JSON                │
│  │   └── MARKET_DATA_metadata.json                                          │
│  ├── ddl/                                                                    │
│  │   ├── FINANCIAL_DATA_create.sql     ← PostgreSQL DDL                     │
│  │   └── MARKET_DATA_create.sql                                             │
│  ├── changes/                                                                │
│  │   ├── FINANCIAL_DATA_changes.log    ← Change history                     │
│  │   └── MARKET_DATA_changes.log                                            │
│  └── index.json                         ← Decrypted master index            │
│                                                                               │
│  ⚠️  Temporary files - NOT tracked by Git                                   │
│  🗑️  Clean up with: python scripts/decrypt_metadata.py --clean             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ View/Debug
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      VIEWING & ANALYSIS                                      │
│                                                                               │
│  # View metadata                                                             │
│  cat metadata/decrypted/schemas/FINANCIAL_DATA_metadata.json                │
│                                                                               │
│  # View DDL                                                                  │
│  cat metadata/decrypted/ddl/FINANCIAL_DATA_create.sql                       │
│                                                                               │
│  # View change history (or use script)                                      │
│  cat metadata/decrypted/changes/FINANCIAL_DATA_changes.log                  │
│  python scripts/view_change_history.py --table FINANCIAL_DATA --obfuscated  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Change Detection Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FIRST EXTRACTION (No Previous Metadata)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    Extract metadata from Snowflake
                                    │
                                    ▼
                    No previous metadata found
                                    │
                                    ▼
                    Encrypt and save metadata
                                    │
                                    ▼
                    Log: "Initial metadata extraction"
                                    │
                                    ▼
                    ┌─────────────────────────────────┐
                    │ metadata/schemas/{id}.enc       │
                    │ metadata/ddl/{id}.enc           │
                    │ metadata/changes/{id}.enc       │
                    └─────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│              SUBSEQUENT EXTRACTION (With Previous Metadata)                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    Extract metadata from Snowflake
                                    │
                                    ▼
                    Decrypt previous metadata
                                    │
                                    ▼
                    Compare old vs new
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
            No Changes Detected            Changes Detected!
                    │                               │
                    ▼                               ▼
            Skip archiving                  Archive old files
                    │                       {id}_{YYYYMMDD}.enc
                    │                               │
                    │                               ▼
                    │                       Log changes to
                    │                       encrypted change log
                    │                               │
                    └───────────────┬───────────────┘
                                    ▼
                    Encrypt and save new metadata
                                    │
                                    ▼
                    ┌─────────────────────────────────┐
                    │ metadata/schemas/{id}.enc       │
                    │ metadata/ddl/{id}.enc           │
                    │ metadata/changes/{id}.enc       │
                    └─────────────────────────────────┘
```

---

## Change Log Structure

```
metadata/changes/{file_id}.enc (encrypted)
    │
    │ Decrypt with password
    ▼
metadata/decrypted/changes/TABLE_NAME_changes.log (plain text)

┌─────────────────────────────────────────────────────────────────────────────┐
│ [2024-01-03T10:30:45Z] Schema change detected                               │
│ Summary: 2 columns added, 1 type changed                                    │
│                                                                               │
│ Changes:                                                                     │
│   + Column added: NEW_COLUMN (VARCHAR(100), NOT NULL)                       │
│   + Column added: ANOTHER_COLUMN (INTEGER, NULL)                            │
│   ~ Column type changed: AMOUNT                                              │
│       Old: NUMBER(18,2)                                                      │
│       New: NUMERIC(18,2)                                                     │
│                                                                               │
│ Archived Files:                                                              │
│   - metadata/schemas/4923cba5118f2c90_20240103.enc                          │
│   - metadata/ddl/18a094ce60d6f8ed_20240103.enc                              │
│                                                                               │
│ ============================================================================ │
│                                                                               │
│ [2024-01-01T08:15:30Z] Initial metadata extraction                          │
│                                                                               │
│ Created Files:                                                               │
│   - metadata/schemas/4923cba5118f2c90.enc                                   │
│   - metadata/ddl/18a094ce60d6f8ed.enc                                       │
│                                                                               │
│ ============================================================================ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Password Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PASSWORD RESOLUTION                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    Script needs password
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
        Command line argument?          .env file configured?
        --password mypassword           ENCRYPTION_PASSWORD=...
                    │                               │
                    │ Yes                           │ Yes
                    ▼                               ▼
            Use provided password           Use .env password
                    │                               │
                    │ No                            │ No
                    └───────────────┬───────────────┘
                                    ▼
                        Prompt for password
                        (secure input, no echo)
                                    │
                                    ▼
                        Use prompted password
```

---

## File ID Generation (Deterministic)

```
Table Name: "FINANCIAL_DATA"
    │
    ▼
SHA-256 Hash("FINANCIAL_DATA:metadata")
    │
    ▼
Take first 16 characters
    │
    ▼
File ID: "4923cba5118f2c90"
    │
    ▼
Metadata: metadata/schemas/4923cba5118f2c90.enc
DDL:      metadata/ddl/18a094ce60d6f8ed.enc
Changes:  metadata/changes/a1b2c3d4e5f6g7h8.enc

✅ Same table always gets same file ID
✅ Enables change tracking across runs
✅ Archived files use: {file_id}_{YYYYMMDD}.enc
```

---

## Integration with Data Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      COMPLETE DATA PIPELINE                                  │
└─────────────────────────────────────────────────────────────────────────────┘

Step 1: Extract Metadata
    python scripts/extract_metadata.py --all --obfuscate --check-changes
    │
    ▼
    metadata/schemas/*.enc
    metadata/ddl/*.enc
    metadata/changes/*.enc

Step 2: Review Changes (Optional)
    python scripts/view_change_history.py --summary
    │
    ▼
    Console output showing schema changes

Step 3: Export Data
    python scripts/export_data.py --all
    │
    ▼
    exports/{table_folder}/
    ├── {chunk_id}.parquet.enc
    ├── {chunk_id}.parquet.enc
    └── manifest.json.enc

Step 4: Transfer Data
    Manual or automated transfer to external system
    │
    ▼
    External system receives encrypted files

Step 5: Import Data
    python scripts/import_data.py --all
    │
    ▼
    PostgreSQL tables populated

All steps use same ENCRYPTION_PASSWORD from .env!
```

---

## Security Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SECURITY LAYERS                                    │
└─────────────────────────────────────────────────────────────────────────────┘

Layer 1: Encryption at Rest
    ├── AES-256-GCM encryption
    ├── PBKDF2 key derivation (100,000 iterations)
    ├── Unique salt per file
    └── Authenticated encryption (prevents tampering)

Layer 2: Obfuscation
    ├── Random file IDs (deterministic per table)
    ├── No table names in filenames
    ├── Master index encrypted separately
    └── Requires password + index to identify tables

Layer 3: Git Safety
    ├── Encrypted files tracked in Git
    ├── Decrypted files excluded (.gitignore)
    ├── .env file excluded (.gitignore)
    └── Temporary files auto-cleaned

Layer 4: Access Control
    ├── Password required for all operations
    ├── Password stored in .env (not in code)
    ├── Secure password prompts (no echo)
    └── No passwords in command history
```

---

## Quick Decision Tree

```
Do you need to view metadata?
    │
    ├─ Yes ─→ Is it encrypted?
    │           │
    │           ├─ Yes ─→ python scripts/decrypt_metadata.py --table TABLE_NAME
    │           │         View files in metadata/decrypted/
    │           │         Clean up: python scripts/decrypt_metadata.py --clean
    │           │
    │           └─ No ──→ cat metadata/schemas/TABLE_NAME_metadata.json
    │
    └─ No ──→ Do you need to check for changes?
                │
                ├─ Yes ─→ python scripts/view_change_history.py --table TABLE_NAME --obfuscated
                │
                └─ No ──→ Do you need to extract metadata?
                            │
                            └─ Yes ─→ python scripts/extract_metadata.py --all --obfuscate --check-changes
```
