# Metadata Directory

This directory contains table metadata extracted from Snowflake, including schemas, DDL files, change logs, and decrypted files.

## Directory Structure

```
metadata/
├── schemas/              # Table metadata (JSON format)
├── ddl/                  # PostgreSQL DDL files (SQL format)
├── changes/              # Change history logs (text format)
├── decrypted/            # Decrypted files (NOT tracked by Git)
└── index.enc             # Master index (when obfuscation is enabled)
```

## File Types

### Encrypted Files (Obfuscation Enabled)
- **Format**: `{random_id}.enc`
- **Example**: `4923cba5118f2c90.enc`
- **Location**: `schemas/` and `ddl/`
- **Purpose**: Secure storage of sensitive metadata
- **Git**: Tracked

### Plain Text Files (Obfuscation Disabled)
- **Format**: `{table_name}_metadata.json` or `{table_name}_create.sql`
- **Example**: `FUND_SHARE_CLASS_BASIC_INFO_metadata.json`
- **Location**: `schemas/` and `ddl/`
- **Purpose**: Human-readable metadata storage
- **Git**: Tracked

### Archived Files
- **Format**: `{file_id}_{YYYYMMDD}.enc` or `{table_name}_{YYYYMMDD}_metadata.json`
- **Example**: `4923cba5118f2c90_20240103.enc`
- **Location**: `schemas/` and `ddl/`
- **Purpose**: Historical versions when schema changes
- **Git**: Tracked

### Change Logs
- **Format**: `{table_name}_changes.log`
- **Example**: `FUND_SHARE_CLASS_BASIC_INFO_changes.log`
- **Location**: `changes/`
- **Purpose**: Persistent record of all metadata changes
- **Git**: Tracked

### Decrypted Files
- **Format**: `{table_name}_metadata.json` or `{table_name}_create.sql`
- **Location**: `decrypted/schemas/` and `decrypted/ddl/`
- **Purpose**: Temporary human-readable versions for debugging
- **Git**: NOT tracked (automatically ignored)

## Usage

### Extract Metadata
```bash
# Without obfuscation
python scripts/extract_metadata.py --all

# With obfuscation
python scripts/extract_metadata.py --all --obfuscate --password <password>

# With change tracking
python scripts/extract_metadata.py --all --check-changes
```

### Decrypt Metadata (Obfuscation Enabled)
```bash
# Decrypt all tables
python scripts/decrypt_metadata.py --all --password <password>

# Decrypt specific table
python scripts/decrypt_metadata.py --table TABLE_NAME --password <password>

# List available tables
python scripts/decrypt_metadata.py --list --password <password>

# Clean up decrypted files
python scripts/decrypt_metadata.py --clean
```

### View Change History
```bash
# View all changes for a table
python scripts/view_change_history.py --table TABLE_NAME

# View last 5 changes
python scripts/view_change_history.py --table TABLE_NAME --limit 5

# View changes in date range
python scripts/view_change_history.py --table TABLE_NAME --from 2024-01-01 --to 2024-01-31

# View summary of all tables
python scripts/view_change_history.py --summary
```

## Security Notes

- **Encrypted files** are safe to commit to Git
- **Decrypted files** are automatically excluded from Git (via `.gitignore`)
- **Change logs** contain table names but no sensitive data
- **Master index** (`index.enc`) maps table names to encrypted file IDs
- Always use strong passwords for encryption
- Never commit decrypted files or passwords

## Change Tracking

When `--check-changes` is enabled during metadata extraction:

1. **First Extraction**: Creates initial metadata files and logs the extraction
2. **Subsequent Extractions**: Compares with previous metadata
3. **Changes Detected**: Archives old files, logs changes, saves new files
4. **No Changes**: Skips archiving and logging

Change logs are written to `changes/{table_name}_changes.log` and include:
- Timestamp (ISO 8601 format)
- Summary of changes
- Detailed change list
- Archived file paths

## Troubleshooting

### Cannot decrypt files
- Verify password is correct
- Ensure `index.enc` exists
- Check that encrypted files exist in `schemas/` and `ddl/`

### Change logs not created
- Ensure `--check-changes` flag is used during extraction
- Check that `metadata/changes/` directory exists
- Verify write permissions

### Decrypted files committed to Git
- Run `python scripts/decrypt_metadata.py --clean`
- Verify `.gitignore` contains `metadata/decrypted/`
- Remove from Git: `git rm -r --cached metadata/decrypted/`

## Related Documentation

- [Metadata Change Tracking Guide](../docs/metadata-change-tracking.md)
- [Command Reference](../docs/command-reference.md)
- [Complete Workflow](../docs/complete-workflow.md)
