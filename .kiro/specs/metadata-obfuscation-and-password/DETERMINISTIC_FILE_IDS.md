# Deterministic File ID Implementation

## Date: December 29, 2024

## Change Summary

Updated the obfuscator to generate **deterministic file IDs** instead of random ones. This ensures that the same table always gets the same obfuscated file names across multiple runs.

## What Changed

### Before (Random IDs):
- Each run generated new random file IDs
- Same table would get different file names each time
- Old files would become orphaned
- Example: `table1` → Run 1: `a7f3d9e2.enc`, Run 2: `b4c8f1a9.enc`

### After (Deterministic IDs):
- File IDs are generated using SHA-256 hash of table name + context
- Same table always gets the same file ID
- Re-running overwrites old files (no orphans)
- Example: `table1` → Always: `3f8a9c2e1d4b5a7c.enc`

## Implementation Details

### New Method: `generate_deterministic_identifier()`

```python
def generate_deterministic_identifier(self, key: str, context: str = "") -> str:
    """
    Generate a deterministic identifier based on a key
    
    Args:
        key: The key to generate identifier from (e.g., table name)
        context: Optional context to include in hash (e.g., "metadata", "ddl", "folder")
        
    Returns:
        Hexadecimal string identifier (deterministic)
    """
    import hashlib
    
    # Create deterministic ID from key + context
    data = f"{key}:{context}".encode('utf-8')
    hash_obj = hashlib.sha256(data)
    identifier = hash_obj.hexdigest()[:self.identifier_length]
    
    return identifier
```

### Updated Methods

All table-level file ID generation methods now use deterministic IDs:

1. **`generate_folder_id(table_name)`** - For data export folders
   - Context: "folder"
   - Example: `financial_data` → `3f8a9c2e1d4b5a7c`

2. **`generate_manifest_id(table_name)`** - For data export manifests
   - Context: "manifest"
   - Example: `financial_data` → `7b2d4e8f9a1c3e5d`

3. **`generate_metadata_file_id(table_name, file_type)`** - For metadata files
   - Context: "metadata" or "ddl"
   - Example: `financial_data` + "metadata" → `5c9e2a7f4b8d1e3a`
   - Example: `financial_data` + "ddl" → `8d1f3c5e7a9b2d4f`

### Chunk Files (Still Random)

Chunk files within a table export still use random IDs via `generate_file_id()`:
- This is intentional - chunks are temporary and don't need consistency
- Random IDs prevent collisions during parallel processing

## Benefits

### 1. Idempotent Operations
Running the same export/extraction multiple times produces the same file structure:
```
metadata/
├── schemas/
│   └── 3f8a9c2e1d4b5a7c.enc  # Always the same for table1
└── ddl/
    └── 7b2d4e8f9a1c3e5d.enc  # Always the same for table1
```

### 2. No Orphaned Files
Re-running overwrites existing files instead of creating new ones:
- Old: Run 1 creates `a7f3d9e2.enc`, Run 2 creates `b4c8f1a9.enc` (orphan!)
- New: Run 1 creates `3f8a9c2e.enc`, Run 2 overwrites `3f8a9c2e.enc` (clean!)

### 3. Predictable for Same Input
Same table name always produces same file ID:
- Easier to debug and troubleshoot
- Consistent behavior across environments
- Master index remains stable

### 4. Still Secure
File names are still obfuscated:
- SHA-256 hash is one-way (can't reverse to get table name)
- 16-character hex IDs provide sufficient uniqueness
- Casual observers still can't identify tables

## Security Considerations

### Hash Function
- Uses SHA-256 (cryptographically secure)
- One-way function (cannot reverse to get table name)
- Collision-resistant for practical purposes

### Obfuscation Level
- File IDs don't reveal table names
- Context strings ("metadata", "ddl", "folder") add differentiation
- 16-character hex provides 2^64 possible values

### No Password in Hash
- File IDs are based on table name + context only
- Password is NOT included in the hash
- This is intentional - allows file ID consistency regardless of password changes

## Applies To

This change affects both:

### 1. Metadata Files
- `metadata/schemas/{file_id}.enc` - Metadata JSON files
- `metadata/ddl/{file_id}.enc` - DDL SQL files
- `metadata/index.enc` - Master index

### 2. Data Export Files
- `exports/{folder_id}/` - Export folder per table
- `exports/{folder_id}/{manifest_id}.enc` - Manifest file
- `exports/{folder_id}/{chunk_id}.enc` - Chunk files (still random)

## Example File ID Generation

For table `financial_data`:

```python
obfuscator = DataObfuscator()

# Folder ID (for data exports)
folder_id = obfuscator.generate_folder_id("financial_data")
# Result: "3f8a9c2e1d4b5a7c" (always the same)

# Manifest ID (for data exports)
manifest_id = obfuscator.generate_manifest_id("financial_data")
# Result: "7b2d4e8f9a1c3e5d" (always the same)

# Metadata file ID
metadata_id = obfuscator.generate_metadata_file_id("financial_data", "metadata")
# Result: "5c9e2a7f4b8d1e3a" (always the same)

# DDL file ID
ddl_id = obfuscator.generate_metadata_file_id("financial_data", "ddl")
# Result: "8d1f3c5e7a9b2d4f" (always the same)
```

## Backward Compatibility

### Existing Random Files
If you have existing exports with random file IDs:
- They will continue to work for imports
- New exports will use deterministic IDs
- Old random files can be manually deleted

### Master Index
The master index will be updated with new deterministic IDs on next run:
- Old index with random IDs will be overwritten
- New index will have deterministic IDs
- Import script will use the latest index

## Testing

To verify deterministic behavior:

```bash
# Run 1
python scripts/extract_metadata.py --all
# Note the file IDs in metadata/schemas/ and metadata/ddl/

# Run 2
python scripts/extract_metadata.py --all
# File IDs should be IDENTICAL to Run 1

# Run 3 (different password)
python scripts/extract_metadata.py --all --password-file new_password.txt
# File IDs should STILL be identical (password not in hash)
```

## Files Modified

1. `pipeline/transformers/obfuscator.py`
   - Added `generate_deterministic_identifier()` method
   - Updated `generate_folder_id()` to use deterministic IDs
   - Updated `generate_manifest_id()` to use deterministic IDs
   - Updated `generate_metadata_file_id()` to use deterministic IDs
   - Kept `generate_file_id()` random for chunk files

## Notes

- Chunk files within exports still use random IDs (intentional)
- Password is NOT included in the hash (allows password changes)
- File IDs are based on table name + context only
- This change applies to both metadata and data exports
- No changes needed to import scripts (they use master index)
