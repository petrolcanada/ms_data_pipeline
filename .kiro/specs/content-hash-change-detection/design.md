# Design Document: Content-Based Change Detection for Data Export

## Overview

This design implements a content-based change detection system for the data export pipeline. The system computes SHA-256 hashes of chunk data before encryption and compares them with existing files to determine if writes are necessary. This prevents unnecessary file modifications, keeping the Git repository clean and reducing disk I/O.

The implementation focuses on minimal changes to the existing `export_data.py` script while adding a new utility module for hash computation and comparison logic.

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Export Table Process                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  For Each Chunk:                                                 │
│  1. Extract data from Snowflake                                  │
│  2. Save to temporary Parquet file                               │
│  3. Compute content hash (SHA-256)                               │
│  4. Check if target encrypted file exists                        │
│     ├─ No → Encrypt and write file (NEW)                        │
│     └─ Yes → Decrypt existing, compute hash, compare            │
│         ├─ Hashes match → Skip write (UNCHANGED)                │
│         └─ Hashes differ → Encrypt and write file (CHANGED)     │
│  5. Collect metadata for manifest                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Generate Manifest:                                              │
│  1. Create manifest JSON with all chunk metadata                 │
│  2. Compute manifest content hash                                │
│  3. Compare with existing manifest (if exists)                   │
│     ├─ Hashes match → Skip write                                │
│     └─ Hashes differ → Write manifest                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Report Statistics:                                              │
│  - Total chunks processed                                        │
│  - Chunks skipped (unchanged)                                    │
│  - Chunks written (new/changed)                                  │
│  - Percentage unchanged                                          │
└─────────────────────────────────────────────────────────────────┘
```

### Component Interaction

```
┌──────────────────┐
│  export_data.py  │
│  (main script)   │
└────────┬─────────┘
         │
         │ uses
         ▼
┌──────────────────────────────────────┐
│  ContentHashComparator               │
│  (new utility class)                 │
│  ─────────────────────────────────   │
│  + compute_file_hash(path)           │
│  + should_write_file(new_hash, ...)  │
│  + decrypt_and_hash(enc_file, ...)   │
└────────┬─────────────────────────────┘
         │
         │ uses
         ▼
┌──────────────────────────────────────┐
│  FileEncryptor                       │
│  (existing)                          │
│  ─────────────────────────────────   │
│  + encrypt_file(...)                 │
│  + decrypt_file(...)                 │
└──────────────────────────────────────┘
```

## Components and Interfaces

### 1. ContentHashComparator Class

**Location:** `pipeline/utils/content_hash_comparator.py`

**Purpose:** Provides utilities for computing content hashes and comparing files to determine if writes are necessary.

**Interface:**

```python
class ContentHashComparator:
    """
    Utility for content-based change detection using SHA-256 hashes.
    """
    
    def __init__(self, encryptor: FileEncryptor):
        """
        Initialize with an encryptor for decrypting existing files.
        
        Args:
            encryptor: FileEncryptor instance for decryption operations
        """
        self.encryptor = encryptor
        self.logger = get_logger(__name__)
    
    def compute_file_hash(self, file_path: Path) -> str:
        """
        Compute SHA-256 hash of a file's contents.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hexadecimal string representation of SHA-256 hash
            
        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If file cannot be read
        """
        pass
    
    def decrypt_and_hash(
        self, 
        encrypted_file: Path, 
        password: str
    ) -> Optional[str]:
        """
        Decrypt an encrypted file and compute its content hash.
        
        Creates a temporary decrypted file, computes hash, then deletes temp file.
        
        Args:
            encrypted_file: Path to encrypted file
            password: Decryption password
            
        Returns:
            SHA-256 hash of decrypted content, or None if decryption fails
        """
        pass
    
    def should_write_file(
        self,
        new_content_hash: str,
        target_encrypted_file: Path,
        password: str
    ) -> tuple[bool, str]:
        """
        Determine if a file should be written based on content comparison.
        
        Args:
            new_content_hash: Hash of the new content to be written
            target_encrypted_file: Path where encrypted file would be written
            password: Password for decrypting existing file
            
        Returns:
            Tuple of (should_write: bool, reason: str)
            - (True, "new_file"): File doesn't exist, should write
            - (True, "content_changed"): Content differs, should write
            - (True, "decryption_failed"): Couldn't decrypt existing, should write
            - (False, "content_unchanged"): Content identical, skip write
        """
        pass
```

### 2. Modified export_table Function

**Location:** `scripts/export_data.py`

**Changes:**
- Initialize `ContentHashComparator` at the start of the function
- Track statistics: `chunks_written`, `chunks_skipped`, `chunks_new`
- For each chunk:
  1. Save to Parquet and compute hash
  2. Call `should_write_file()` to determine if write is needed
  3. Only encrypt and write if necessary
  4. Collect appropriate metadata (from new or existing file)
- Display statistics at the end

**Key Code Changes:**

```python
# Initialize comparator
comparator = ContentHashComparator(encryptor)

# Statistics tracking
chunks_written = 0
chunks_skipped = 0
chunks_new = 0

# In chunk processing loop:
# 1. Save Parquet and compute hash
parquet_info = extractor.save_chunk_to_parquet(...)
content_hash = comparator.compute_file_hash(parquet_file)

# 2. Check if write is needed
should_write, reason = comparator.should_write_file(
    content_hash,
    encrypted_file,
    password
)

# 3. Conditionally write
if should_write:
    if reason == "new_file":
        chunks_new += 1
    else:
        chunks_written += 1
    
    # Encrypt and write
    encryption_info = encryptor.encrypt_file(...)
    chunk_metadata = {...}  # Use new file info
else:
    chunks_skipped += 1
    # Read existing file metadata
    chunk_metadata = {...}  # Use existing file info

# Remove temp Parquet
parquet_file.unlink()
```

### 3. Manifest Change Detection

**Location:** `scripts/export_data.py` (in `export_table` function)

**Implementation:**
- After creating manifest dictionary, serialize to JSON string
- Compute hash of JSON string
- If manifest file exists (encrypted or plain):
  - Read and hash existing manifest content
  - Compare hashes
  - Skip write if identical
- Track manifest write/skip in statistics

## Data Models

### Export Statistics

```python
@dataclass
class ExportStatistics:
    """Statistics for an export operation."""
    total_chunks: int
    chunks_new: int          # Files that didn't exist before
    chunks_changed: int      # Files that existed but content changed
    chunks_unchanged: int    # Files that were skipped (identical)
    manifest_written: bool   # Whether manifest was written
    
    @property
    def chunks_written(self) -> int:
        """Total chunks written (new + changed)."""
        return self.chunks_new + self.chunks_changed
    
    @property
    def unchanged_percentage(self) -> float:
        """Percentage of chunks that were unchanged."""
        if self.total_chunks == 0:
            return 0.0
        return (self.chunks_unchanged / self.total_chunks) * 100
```

### Chunk Metadata Enhancement

The existing chunk metadata structure remains the same, but we'll add an internal flag during processing:

```python
{
    "chunk_number": int,
    "file": str,
    "rows": int,
    "size_bytes": int,
    "checksum_sha256": str,  # Existing field (encryption checksum)
    "encrypted": bool,
    # Internal use only (not saved to manifest):
    "_content_hash": str,     # SHA-256 of unencrypted content
    "_was_skipped": bool      # Whether file write was skipped
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Hash Consistency

*For any* Parquet file content, computing the SHA-256 hash multiple times should produce identical hash values.

**Validates: Requirements 1.3**

### Property 2: Identical Content Detection

*For any* chunk data, if the data is exported twice without changes, the content hash of the second export should match the first, and the file write should be skipped.

**Validates: Requirements 2.3, 3.1**

### Property 3: Changed Content Detection

*For any* two different chunk datasets, their content hashes should differ, and the system should write the new file.

**Validates: Requirements 2.3, 3.2**

### Property 4: Decryption Round-Trip

*For any* encrypted file created by the export system, decrypting it and computing the content hash should produce the same hash as the original unencrypted content.

**Validates: Requirements 2.2**

### Property 5: Statistics Accuracy

*For any* export operation, the sum of chunks_new + chunks_changed + chunks_unchanged should equal total_chunks.

**Validates: Requirements 4.1, 4.2, 4.3**

### Property 6: Manifest Consistency

*For any* export operation, the manifest should contain metadata for exactly the number of chunks processed, regardless of how many were skipped.

**Validates: Requirements 5.1, 5.4**

### Property 7: File Existence Handling

*For any* chunk, if the target encrypted file does not exist, the system should write it and classify it as "new".

**Validates: Requirements 3.3**

### Property 8: Corruption Resilience

*For any* corrupted existing file that fails decryption, the system should write the new file and not halt the export process.

**Validates: Requirements 7.1, 7.2, 7.3**

## Error Handling

### Decryption Failures

**Scenario:** Existing encrypted file is corrupted or uses wrong password

**Handling:**
1. Log warning: "Failed to decrypt existing file {filename}: {error}"
2. Return `(True, "decryption_failed")` from `should_write_file()`
3. Write new encrypted file
4. Continue export process

**Code:**
```python
try:
    existing_hash = comparator.decrypt_and_hash(encrypted_file, password)
except Exception as e:
    logger.warning(f"Failed to decrypt {encrypted_file.name}: {e}")
    return (True, "decryption_failed")
```

### Hash Computation Failures

**Scenario:** Cannot read file to compute hash

**Handling:**
1. Log error: "Failed to compute hash for {filename}: {error}"
2. Raise exception (this indicates a serious problem)
3. Let caller handle (may retry or fail export)

**Code:**
```python
try:
    with open(file_path, 'rb') as f:
        # Compute hash
except IOError as e:
    logger.error(f"Failed to read {file_path}: {e}")
    raise
```

### Temporary File Cleanup

**Scenario:** Temporary decrypted files need cleanup even on errors

**Handling:**
1. Use try/finally blocks
2. Ensure temp files are deleted even if hash computation fails
3. Log cleanup actions

**Code:**
```python
temp_file = None
try:
    temp_file = Path(f"{encrypted_file}.tmp")
    self.encryptor.decrypt_file(encrypted_file, temp_file, password)
    content_hash = self.compute_file_hash(temp_file)
    return content_hash
finally:
    if temp_file and temp_file.exists():
        temp_file.unlink()
```

## Testing Strategy

### Unit Tests

**Test File:** `tests/test_content_hash_comparator.py`

**Test Cases:**
1. `test_compute_file_hash_consistency`: Verify same file produces same hash
2. `test_compute_file_hash_different_content`: Verify different files produce different hashes
3. `test_decrypt_and_hash_success`: Verify decryption and hashing works
4. `test_decrypt_and_hash_failure`: Verify graceful handling of decryption failures
5. `test_should_write_file_new`: Verify returns True for non-existent files
6. `test_should_write_file_unchanged`: Verify returns False for identical content
7. `test_should_write_file_changed`: Verify returns True for changed content
8. `test_should_write_file_corrupted`: Verify returns True for corrupted existing files

### Property-Based Tests

**Test File:** `tests/test_content_hash_properties.py`

**Property Tests:**
1. **Hash Consistency Property**: Generate random binary data, compute hash twice, verify identical
2. **Round-Trip Property**: Generate random data, encrypt, decrypt, hash - verify matches original hash
3. **Statistics Accuracy Property**: Generate random export scenarios, verify sum of statistics equals total
4. **Different Content Property**: Generate two different random datasets, verify hashes differ

### Integration Tests

**Test File:** `tests/test_export_change_detection_integration.py`

**Test Scenarios:**
1. `test_export_twice_no_changes`: Export same data twice, verify second run skips all files
2. `test_export_with_data_changes`: Export, modify source data, export again, verify files are rewritten
3. `test_export_with_partial_changes`: Export, modify some chunks, verify only changed chunks are written
4. `test_export_with_corrupted_existing`: Create corrupted file, export, verify file is replaced
5. `test_export_statistics_reporting`: Verify statistics are correctly calculated and displayed
6. `test_manifest_change_detection`: Verify manifest is only written when content changes

### Manual Testing Checklist

1. **First Export**: Run export, verify all files created
2. **Repeat Export**: Run export again with same data, verify all files skipped
3. **Data Change**: Modify source data, run export, verify only changed files written
4. **Clean Flag**: Run with `--clean`, verify all files deleted and recreated
5. **Obfuscation**: Test with both `--no-obfuscate` and default obfuscation
6. **Multiple Tables**: Test with `--all`, verify statistics per table
7. **Git Status**: After repeat export, verify `git status` shows no changes

## Performance Considerations

### Hash Computation Overhead

- **SHA-256 computation**: ~500 MB/s on modern hardware
- **For 100MB file**: ~200ms computation time
- **Acceptable overhead**: < 5% of total export time

### Decryption for Comparison

- **Only when file exists**: No overhead for new files
- **Temporary file approach**: Simple, reliable, minimal memory
- **Alternative considered**: In-memory decryption (more complex, marginal benefit)

### Optimization Strategies

1. **Reuse encryptor instance**: Avoid re-initializing crypto primitives
2. **Stream-based hashing**: Process files in chunks to limit memory usage
3. **Skip manifest comparison**: If no chunks changed, manifest likely unchanged (future optimization)

## Backward Compatibility

### Existing Export Folders

- System works with any existing export folder structure
- No migration needed
- First run after upgrade will compare all files
- Subsequent runs benefit from change detection

### File Format Compatibility

- No changes to encrypted file format
- No changes to manifest structure
- Works with both obfuscated and non-obfuscated naming
- Works with both encrypted and plain manifests

### Command-Line Interface

- No breaking changes to CLI arguments
- `--clean` flag behavior unchanged (deletes everything)
- All existing workflows continue to work

## Future Enhancements

### Manifest-Based Fast Path

**Idea:** Store content hashes in manifest, skip decryption for comparison

**Benefits:**
- Faster comparison (no decryption needed)
- Reduced I/O

**Challenges:**
- Manifest format change (backward compatibility)
- Manifest must be trusted (security consideration)

### Parallel Hash Computation

**Idea:** Compute hashes for multiple existing files in parallel

**Benefits:**
- Faster for large exports with many unchanged files

**Challenges:**
- More complex code
- Limited by disk I/O, not CPU

### Smart Manifest Detection

**Idea:** If no chunks changed, skip manifest comparison

**Benefits:**
- Saves one file comparison per export

**Implementation:**
- Track if any chunks were written
- If `chunks_written == 0`, skip manifest write
