# Design Document

## Overview

This design enhances the metadata management system with three key capabilities:
1. **Persistent change logging** - Write metadata changes to permanent log files
2. **Decryption utilities** - Decrypt encrypted metadata for human viewing
3. **Obfuscation-aware change tracking** - Make change detection work with encrypted files

The design maintains backward compatibility with existing workflows while adding powerful new debugging and auditing capabilities.

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Metadata Extraction Flow                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Extract from    │
                    │   Snowflake      │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Compare with    │
                    │  Previous (if    │
                    │  check-changes)  │
                    └──────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
            ┌──────────────┐    ┌──────────────┐
            │  No Changes  │    │   Changes    │
            │              │    │   Detected   │
            └──────────────┘    └──────────────┘
                    │                   │
                    │                   ▼
                    │           ┌──────────────┐
                    │           │  Archive Old │
                    │           │    Files     │
                    │           └──────────────┘
                    │                   │
                    │                   ▼
                    │           ┌──────────────┐
                    │           │ Write Change │
                    │           │   to Log     │
                    │           └──────────────┘
                    │                   │
                    └───────────┬───────┘
                                ▼
                    ┌──────────────────┐
                    │  Save Metadata   │
                    │  (Encrypted or   │
                    │   Plain Text)    │
                    └──────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    Decryption Utility Flow                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Decrypt Master  │
                    │      Index       │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Find Table(s)   │
                    │   to Decrypt     │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Decrypt Files   │
                    │  (Metadata +     │
                    │      DDL)        │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Save to         │
                    │  metadata/       │
                    │  decrypted/      │
                    └──────────────────┘
```

### Directory Structure

```
metadata/
├── schemas/                          # Encrypted/plain metadata files
│   ├── 4923cba5118f2c90.enc         # Current (obfuscated)
│   ├── 4923cba5118f2c90_20240103.enc # Archived (obfuscated)
│   ├── TABLE_NAME_metadata.json      # Current (non-obfuscated)
│   └── TABLE_NAME_20240103_metadata.json # Archived (non-obfuscated)
│
├── ddl/                              # Encrypted/plain DDL files
│   ├── 18a094ce60d6f8ed.enc         # Current (obfuscated)
│   ├── 18a094ce60d6f8ed_20240103.enc # Archived (obfuscated)
│   ├── TABLE_NAME_create.sql         # Current (non-obfuscated)
│   └── TABLE_NAME_20240103_create.sql # Archived (non-obfuscated)
│
├── changes/                          # Persistent change logs (Git-tracked)
│   ├── TABLE_NAME_changes.log
│   └── ANOTHER_TABLE_changes.log
│
├── decrypted/                        # Human-readable files (NOT Git-tracked)
│   ├── schemas/
│   │   ├── TABLE_NAME_metadata.json
│   │   └── ANOTHER_TABLE_metadata.json
│   ├── ddl/
│   │   ├── TABLE_NAME_create.sql
│   │   └── ANOTHER_TABLE_create.sql
│   └── index.json                    # Decrypted master index
│
├── index.enc                         # Master index (obfuscated mode only)
└── README.md                         # Directory documentation
```

## Components and Interfaces

### 1. Enhanced ChangeLogger

**File:** `pipeline/utils/change_logger.py`

**Purpose:** Write metadata changes to persistent log files

**Key Methods:**

```python
class ChangeLogger:
    def __init__(self, log_dir: Path = Path("metadata/changes")):
        """Initialize with log directory"""
        
    def log_change(
        self,
        table_name: str,
        changes: List[Dict],
        summary: str,
        archived_files: Optional[Dict[str, Path]] = None
    ) -> None:
        """
        Write change to persistent log file
        
        Args:
            table_name: Name of the table
            changes: List of change dictionaries from MetadataComparator
            summary: Human-readable summary
            archived_files: Dict with 'metadata' and 'ddl' archived file paths
        """
        
    def log_initial_extraction(self, table_name: str) -> None:
        """Log first-time metadata extraction"""
        
    def get_change_history(
        self,
        table_name: str,
        limit: Optional[int] = None
    ) -> List[str]:
        """
        Retrieve change history for a table
        
        Args:
            table_name: Name of the table
            limit: Maximum number of entries to return (most recent first)
            
        Returns:
            List of log entries
        """
        
    def get_changes_by_date_range(
        self,
        table_name: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[str]:
        """Get changes within a date range"""
        
    def format_change_entry(
        self,
        timestamp: str,
        summary: str,
        changes: List[Dict],
        archived_files: Optional[Dict[str, Path]] = None
    ) -> str:
        """Format a single change entry for the log file"""
```

**Log File Format:**

```
[2024-01-03T10:30:45Z] Schema change detected
Summary: 2 columns added, 1 type changed

Changes:
  + Column added: NEW_COLUMN (VARCHAR(100), NOT NULL)
  + Column added: ANOTHER_COLUMN (INTEGER, NULL)
  ~ Column type changed: AMOUNT
      Old: NUMBER(18,2)
      New: NUMERIC(18,2)

Archived Files:
  - metadata/schemas/TABLE_NAME_20240103_metadata.json
  - metadata/ddl/TABLE_NAME_20240103_create.sql

================================================================================

[2024-01-05T14:22:10Z] Initial metadata extraction

Created Files:
  - metadata/schemas/TABLE_NAME_metadata.json
  - metadata/ddl/TABLE_NAME_create.sql

================================================================================
```

### 2. Metadata Decryption Utility

**File:** `pipeline/utils/metadata_decryptor.py`

**Purpose:** Decrypt encrypted metadata files for human viewing

**Key Methods:**

```python
class MetadataDecryptor:
    def __init__(
        self,
        encrypted_dir: Path = Path("metadata"),
        decrypted_dir: Path = Path("metadata/decrypted")
    ):
        """Initialize decryptor with directory paths"""
        
    def decrypt_all_tables(self, password: str) -> Dict[str, Any]:
        """
        Decrypt all tables from master index
        
        Args:
            password: Decryption password
            
        Returns:
            Dictionary with decryption results for each table
        """
        
    def decrypt_table(
        self,
        table_name: str,
        password: str
    ) -> Dict[str, Any]:
        """
        Decrypt metadata and DDL for a specific table
        
        Args:
            table_name: Name of the table to decrypt
            password: Decryption password
            
        Returns:
            Dictionary with decrypted file paths
        """
        
    def decrypt_master_index(self, password: str) -> Dict:
        """Decrypt and return master index"""
        
    def clean_decrypted_files(self) -> Dict[str, int]:
        """
        Delete all decrypted files
        
        Returns:
            Dictionary with count of deleted files by type
        """
        
    def list_available_tables(self, password: str) -> List[str]:
        """List all tables in the master index"""
        
    def ensure_gitignore(self) -> None:
        """Ensure decrypted directory is in .gitignore"""
```

### 3. Enhanced Metadata Extractor

**File:** `pipeline/extractors/metadata_extractor.py`

**Updates:**

```python
class SnowflakeMetadataExtractor:
    def check_metadata_changed_obfuscated(
        self,
        table_name: str,
        new_metadata: Dict[str, Any],
        password: str
    ) -> Optional[Dict[str, Any]]:
        """
        Check for changes when obfuscation is enabled
        
        Decrypts the previous metadata file, compares with new metadata,
        then returns comparison results.
        
        Args:
            table_name: Name of the table
            new_metadata: Newly extracted metadata
            password: Decryption password
            
        Returns:
            Comparison result dict or None if first extraction
        """
        
    def archive_old_metadata_obfuscated(
        self,
        table_name: str,
        password: str
    ) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Archive obfuscated metadata files with timestamp
        
        Args:
            table_name: Name of the table
            password: Decryption password for reading master index
            
        Returns:
            Tuple of (archived_metadata_path, archived_ddl_path)
        """
        
    def save_metadata_to_file(
        self,
        metadata: Dict[str, Any],
        table_name: str,
        check_changes: bool = False,
        password: Optional[str] = None
    ) -> Tuple[Path, Optional[Dict[str, Any]]]:
        """
        Enhanced to:
        - Support change checking with obfuscated files
        - Pass archived file paths to ChangeLogger
        - Write to persistent change log
        """
```

### 4. Decryption Script

**File:** `scripts/decrypt_metadata.py`

**Purpose:** Command-line interface for decrypting metadata

**Interface:**

```bash
# Decrypt all tables
python scripts/decrypt_metadata.py --all --password <password>

# Decrypt specific table
python scripts/decrypt_metadata.py --table TABLE_NAME --password <password>

# Prompt for password (secure input)
python scripts/decrypt_metadata.py --all

# List available tables
python scripts/decrypt_metadata.py --list --password <password>

# Clean up decrypted files
python scripts/decrypt_metadata.py --clean

# Decrypt and view change history
python scripts/decrypt_metadata.py --table TABLE_NAME --password <password> --show-changes
```

**Arguments:**
- `--all`: Decrypt all tables
- `--table <name>`: Decrypt specific table
- `--password <pwd>`: Decryption password (optional, will prompt if not provided)
- `--list`: List available tables in master index
- `--clean`: Delete all decrypted files
- `--show-changes`: Display change history after decrypting
- `--output-dir <path>`: Custom output directory (default: metadata/decrypted)

### 5. Change Log Viewer Script

**File:** `scripts/view_change_history.py`

**Purpose:** View change history for tables

**Interface:**

```bash
# View all changes for a table
python scripts/view_change_history.py --table TABLE_NAME

# View last N changes
python scripts/view_change_history.py --table TABLE_NAME --limit 5

# View changes in date range
python scripts/view_change_history.py --table TABLE_NAME --from 2024-01-01 --to 2024-01-31

# View summary of all tables with changes
python scripts/view_change_history.py --summary
```

## Data Models

### Change Log Entry Structure

```python
{
    "timestamp": "2024-01-03T10:30:45Z",
    "table_name": "FUND_SHARE_CLASS_BASIC_INFO",
    "summary": "2 columns added, 1 type changed",
    "changes": [
        {
            "type": "column_added",
            "column": "NEW_COLUMN",
            "details": {
                "data_type": "VARCHAR(100)",
                "nullable": False,
                "position": 15
            }
        },
        {
            "type": "column_type_changed",
            "column": "AMOUNT",
            "details": {
                "old_type": "NUMBER(18,2)",
                "new_type": "NUMERIC(18,2)"
            }
        }
    ],
    "archived_files": {
        "metadata": "metadata/schemas/TABLE_NAME_20240103_metadata.json",
        "ddl": "metadata/ddl/TABLE_NAME_20240103_create.sql"
    }
}
```

### Decryption Result Structure

```python
{
    "table_name": "FUND_SHARE_CLASS_BASIC_INFO",
    "status": "success",
    "decrypted_files": {
        "metadata": "metadata/decrypted/schemas/FUND_SHARE_CLASS_BASIC_INFO_metadata.json",
        "ddl": "metadata/decrypted/ddl/FUND_SHARE_CLASS_BASIC_INFO_create.sql"
    },
    "metadata_summary": {
        "columns": 25,
        "row_count": 150000,
        "last_altered": "2024-01-03T08:15:30"
    }
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Change Log Persistence
*For any* metadata change detected, writing the change to the log file and then reading the log file should contain that change entry.
**Validates: Requirements 1.1, 1.3**

### Property 2: Change Log Append Behavior
*For any* existing change log file, appending a new entry should increase the file size and preserve all existing entries.
**Validates: Requirements 1.3**

### Property 3: Decryption Round Trip
*For any* encrypted metadata file, decrypting it with the correct password and then comparing with the original plaintext should produce identical content.
**Validates: Requirements 3.1, 3.4**

### Property 4: Decrypted File Naming
*For any* table name, the decrypted metadata filename should contain the original table name, not the obfuscated file ID.
**Validates: Requirements 3.3**

### Property 5: Git Ignore Protection
*For any* decrypted file created, checking if `metadata/decrypted/` is in `.gitignore` should return true.
**Validates: Requirements 4.2**

### Property 6: Change Detection with Obfuscation
*For any* table with obfuscated metadata, if the schema changes, the change detection should work identically to non-obfuscated mode.
**Validates: Requirements 5.1, 5.5**

### Property 7: Archive File Naming
*For any* archived file (obfuscated or not), the filename should contain a timestamp in YYYYMMDD format.
**Validates: Requirements 5.4**

### Property 8: Master Index Consistency
*For any* table in the master index, the metadata_file_id and ddl_file_id should be deterministic across multiple runs.
**Validates: Requirements 5.3**

### Property 9: Change Log Format Consistency
*For any* change log entry, parsing the timestamp should succeed and the format should match ISO 8601.
**Validates: Requirements 1.4**

### Property 10: Decryption Error Handling
*For any* encrypted file, attempting to decrypt with an incorrect password should produce a clear error message, not crash.
**Validates: Requirements 3.6, 10.1**

### Property 11: Change History Retrieval
*For any* table with N change log entries, retrieving the change history with limit M (where M < N) should return exactly M entries.
**Validates: Requirements 8.2**

### Property 12: Directory Creation Idempotence
*For any* directory path, calling the directory creation function multiple times should succeed without errors.
**Validates: Requirements 1.7, 4.1**

## Error Handling

### Decryption Errors

1. **Incorrect Password**
   - Error: `DecryptionError: Failed to decrypt file. Incorrect password or corrupted file.`
   - Action: Prompt user to retry with correct password

2. **Missing Master Index**
   - Error: `FileNotFoundError: Master index not found at metadata/index.enc. Run metadata extraction with obfuscation enabled first.`
   - Action: Guide user to extract metadata

3. **Table Not Found**
   - Error: `TableNotFoundError: Table 'XYZ' not found in master index. Available tables: [list]`
   - Action: Display available tables

### Change Logging Errors

1. **Permission Denied**
   - Error: `PermissionError: Cannot write to metadata/changes/TABLE_NAME_changes.log`
   - Action: Log warning, continue extraction (non-fatal)

2. **Disk Full**
   - Error: `OSError: No space left on device`
   - Action: Log error, continue extraction (non-fatal)

### File System Errors

1. **Cannot Create Directory**
   - Error: `OSError: Cannot create directory metadata/decrypted/`
   - Action: Display error, exit gracefully

2. **File Already Exists**
   - Action: Overwrite decrypted files (they're temporary)

## Testing Strategy

### Unit Tests

**Test File:** `tests/test_change_logger.py`

- Test change log file creation
- Test change log entry formatting
- Test change log appending
- Test change history retrieval
- Test date range filtering
- Test handling of missing log files

**Test File:** `tests/test_metadata_decryptor.py`

- Test master index decryption
- Test single table decryption
- Test all tables decryption
- Test decrypted file naming
- Test cleanup functionality
- Test gitignore management
- Test error handling for wrong passwords

**Test File:** `tests/test_obfuscated_change_tracking.py`

- Test change detection with obfuscated files
- Test archiving obfuscated files
- Test deterministic file ID generation
- Test master index updates

### Property-Based Tests

**Test File:** `tests/property_test_change_logging.py`

Configure each test to run minimum 100 iterations.

**Property Test 1: Change Log Persistence**
- **Feature: metadata-decryption-and-change-logging, Property 1: Change Log Persistence**
- Generate random change dictionaries
- Write to log file
- Read back and verify presence

**Property Test 2: Change Log Append Behavior**
- **Feature: metadata-decryption-and-change-logging, Property 2: Change Log Append Behavior**
- Generate random existing log content
- Append new entry
- Verify all entries present

**Property Test 3: Decryption Round Trip**
- **Feature: metadata-decryption-and-change-logging, Property 3: Decryption Round Trip**
- Generate random metadata dictionaries
- Encrypt then decrypt
- Verify content matches

**Property Test 4: Decrypted File Naming**
- **Feature: metadata-decryption-and-change-logging, Property 4: Decrypted File Naming**
- Generate random table names
- Decrypt and check filename contains table name

**Property Test 5: Git Ignore Protection**
- **Feature: metadata-decryption-and-change-logging, Property 5: Git Ignore Protection**
- Create decrypted files
- Verify .gitignore contains decrypted path

**Property Test 6: Change Detection with Obfuscation**
- **Feature: metadata-decryption-and-change-logging, Property 6: Change Detection with Obfuscation**
- Generate random metadata changes
- Test with both obfuscated and non-obfuscated modes
- Verify identical change detection results

**Property Test 7: Archive File Naming**
- **Feature: metadata-decryption-and-change-logging, Property 7: Archive File Naming**
- Generate random archive operations
- Verify timestamp format in filename

**Property Test 8: Master Index Consistency**
- **Feature: metadata-decryption-and-change-logging, Property 8: Master Index Consistency**
- Generate file IDs for same table multiple times
- Verify deterministic results

**Property Test 9: Change Log Format Consistency**
- **Feature: metadata-decryption-and-change-logging, Property 9: Change Log Format Consistency**
- Generate random change log entries
- Parse timestamps
- Verify ISO 8601 format

**Property Test 10: Decryption Error Handling**
- **Feature: metadata-decryption-and-change-logging, Property 10: Decryption Error Handling**
- Generate random encrypted files
- Attempt decryption with wrong passwords
- Verify graceful error handling

**Property Test 11: Change History Retrieval**
- **Feature: metadata-decryption-and-change-logging, Property 11: Change History Retrieval**
- Generate random change logs with N entries
- Retrieve with limit M < N
- Verify exactly M entries returned

**Property Test 12: Directory Creation Idempotence**
- **Feature: metadata-decryption-and-change-logging, Property 12: Directory Creation Idempotence**
- Call directory creation multiple times
- Verify no errors

### Integration Tests

- Test full workflow: extract → detect changes → log → decrypt → view
- Test with real Snowflake schema changes
- Test obfuscated and non-obfuscated modes
- Test password-protected workflows

## Implementation Notes

### Backward Compatibility

- All new features are opt-in
- Existing metadata extraction works without changes
- Change logging only activates when `--check-changes` is used
- Decryption is a separate utility, doesn't affect extraction

### Performance Considerations

- Change log writes are append-only (fast)
- Decryption is on-demand (doesn't slow extraction)
- Master index is cached in memory during batch operations
- File I/O is minimized through buffering

### Security Considerations

- Decrypted files are never committed to Git
- Password input uses secure prompts (no echo)
- Encrypted files remain encrypted at rest
- Change logs contain table names but no sensitive data

## Future Enhancements

- Email/Slack notifications for changes
- Web UI for viewing change history
- Automated Git commits on changes
- Schema diff visualization
- Change impact analysis
- Rollback capability
