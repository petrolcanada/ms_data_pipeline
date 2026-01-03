# Change Logs Directory

This directory contains persistent change logs for table metadata.

## Purpose

Change logs provide a permanent audit trail of all metadata changes detected from Snowflake. Each table has its own log file that records:

- Schema changes (columns added/removed/modified)
- Data type changes
- Constraint changes (primary keys, foreign keys)
- Initial metadata extractions
- Timestamps for all events

## File Format

**Filename**: `{table_name}_changes.log`

**Example**: `FUND_SHARE_CLASS_BASIC_INFO_changes.log`

## Log Entry Format

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
```

## Change Symbols

- `+` Column added
- `-` Column removed
- `~` Column modified (type, nullable, position, etc.)

## Viewing Change History

### View all changes for a table
```bash
python scripts/view_change_history.py --table TABLE_NAME
```

### View last N changes
```bash
python scripts/view_change_history.py --table TABLE_NAME --limit 5
```

### View changes in date range
```bash
python scripts/view_change_history.py --table TABLE_NAME --from 2024-01-01 --to 2024-01-31
```

### View summary of all tables
```bash
python scripts/view_change_history.py --summary
```

## When Are Changes Logged?

Changes are logged when:

1. **Metadata extraction** is run with `--check-changes` flag
2. **Schema changes** are detected compared to previous extraction
3. **Initial extraction** occurs for a new table

Changes are NOT logged when:
- Extraction runs without `--check-changes` flag
- No schema changes are detected
- Extraction fails or is interrupted

## Log File Management

### Automatic Management
- Log files are created automatically when first change is detected
- Entries are appended (never overwritten)
- Directory is created automatically if it doesn't exist

### Manual Management
- Safe to delete individual log files (history will be lost)
- Safe to archive old log files
- Can be committed to Git for version control

## Git Tracking

✅ **Change logs SHOULD be tracked in Git**

Benefits:
- Permanent audit trail
- Team visibility of schema changes
- Historical record across environments

## Troubleshooting

### No log files created
- Ensure `--check-changes` flag is used during extraction
- Verify write permissions on `metadata/changes/` directory
- Check console output for errors

### Log files not updating
- Verify schema actually changed in Snowflake
- Check that extraction completed successfully
- Ensure previous metadata files exist for comparison

### Cannot read log files
- Check file encoding (should be UTF-8)
- Verify file permissions
- Ensure file is not corrupted

## Related Documentation

- [Metadata Change Tracking Guide](../../docs/metadata-change-tracking.md)
- [View Change History Script](../../scripts/view_change_history.py)
- [Metadata Directory README](../README.md)
