# Metadata Change Tracking Guide

This guide explains how to track and monitor changes to Snowflake table metadata over time.

## Overview

The metadata change tracking feature automatically detects when table schemas change in Snowflake and:
- **Alerts** you with detailed change information
- **Archives** old metadata and DDL files with timestamps
- **Logs** all changes to a persistent change history
- **Preserves** current files while keeping versioned history

## Quick Start

### Check for Metadata Changes

```bash
# Check all configured tables for metadata changes
python scripts/extract_metadata.py --all --check-changes
```

### Force Re-extraction

```bash
# Force re-extraction even if no changes detected
python scripts/extract_metadata.py --all --check-changes --force
```

## Features

### 1. Change Detection

The system compares newly extracted metadata against existing metadata files and detects:

- **Column additions** - New columns added to the table
- **Column removals** - Columns removed from the table
- **Type changes** - Data type modifications (e.g., VARCHAR → TEXT)
- **Nullable changes** - NULL/NOT NULL constraint changes
- **Position changes** - Column order changes
- **Constraint changes** - Primary key and other constraint modifications

### 2. Automatic Archiving

When changes are detected, old files are automatically archived with timestamps:

```
metadata/schemas/
├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata.json          # Current
├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_metadata.json # Archived
└── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241215_metadata.json # Archived

metadata/ddl/
├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create.sql             # Current
├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_create.sql    # Archived
└── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241215_create.sql    # Archived
```

**Naming Convention**: `{table_name}_{YYYYMMDD}_metadata.json` and `{table_name}_{YYYYMMDD}_create.sql`

### 3. Change Logging

All changes are logged to persistent log files in `metadata/changes/`:

```
metadata/changes/
├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_changes.log
└── financial_data_changes.log
```

Each log entry includes:
- Timestamp of the change
- Summary of changes
- Detailed list of all modifications
- Formatted for easy reading and auditing

### 4. Change Alerts

When changes are detected, you'll see a detailed alert:

```
======================================================================
⚠️  METADATA CHANGES DETECTED!
======================================================================

Table: FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
Summary: 2 columns added, 1 type changed

Detailed Changes:
  + Column added: NEW_COLUMN (VARCHAR(100))
  + Column added: ANOTHER_COLUMN (INTEGER)
  ~ Type changed: AMOUNT
      NUMBER(18,2) → NUMERIC(18,2)

Archived old metadata:
  • metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_metadata.json
  • metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_create.sql

======================================================================
```

## Command Reference

### Basic Commands

```bash
# Extract metadata without change checking (default behavior)
python scripts/extract_metadata.py --all

# Extract metadata with change checking
python scripts/extract_metadata.py --all --check-changes

# Force re-extraction even if unchanged
python scripts/extract_metadata.py --all --check-changes --force

# Extract and create PostgreSQL tables
python scripts/extract_metadata.py --all --check-changes --create-postgres
```

### Command Options

| Option | Description |
|--------|-------------|
| `--all` | Extract metadata for all configured tables |
| `--check-changes` | Enable change detection and alerting |
| `--force` | Force re-extraction even if no changes detected |
| `--create-postgres` | Also create PostgreSQL tables after extraction |
| `--drop-existing` | Drop existing PostgreSQL tables before creation |

## Workflow Examples

### Daily Metadata Check

Run this daily to monitor for schema changes:

```bash
python scripts/extract_metadata.py --all --check-changes
```

**Output when no changes**:
```
Extracting metadata for all configured tables...
Change detection enabled - will alert on metadata changes

✓ No metadata changes detected for any tables

Metadata Extraction Results:
==================================================
✓ [UNCHANGED] FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
  Columns: 45
  Rows: 50,000
  Metadata: metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata.json
  DDL: metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create.sql
```

**Output when changes detected**:
```
======================================================================
⚠️  METADATA CHANGES DETECTED!
======================================================================

Table: FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
Summary: 1 column added

Detailed Changes:
  + Column added: RISK_RATING (VARCHAR(50))

Archived old metadata:
  • metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_metadata.json
  • metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_create.sql

======================================================================

Metadata Extraction Results:
==================================================
✓ [CHANGED] FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
  Columns: 46
  Rows: 50,000
  Metadata: metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata.json
  DDL: metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create.sql
```

### First-Time Extraction

When extracting metadata for the first time:

```bash
python scripts/extract_metadata.py --all --check-changes
```

**Output**:
```
Metadata Extraction Results:
==================================================
✓ [NEW] FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
  Columns: 45
  Rows: 50,000
  Metadata: metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_metadata.json
  DDL: metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_create.sql
```

### Review Change History

View the complete change history for a table:

```bash
# View change log
type metadata\changes\FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_changes.log

# Or on Unix/Mac
cat metadata/changes/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_changes.log
```

**Example log content**:
```
================================================================================
[2024-12-28 10:30:00] INITIAL EXTRACTION
================================================================================
Table: FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
Columns: 45
Row Count: 50,000

================================================================================
[2024-12-28 14:15:00] METADATA CHANGE DETECTED
================================================================================
Summary: 1 column added

Changes:
  • Column added: RISK_RATING (VARCHAR(50), nullable=True, position=46)

================================================================================
```

## Integration with Git

### Tracking Archived Files

Add archived metadata files to Git to maintain version history:

```bash
# Add all metadata files
git add metadata/schemas/*.json
git add metadata/ddl/*.sql
git add metadata/changes/*.log

# Commit with descriptive message
git commit -m "Metadata change: Added RISK_RATING column to FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

### Automated Git Workflow

You can create a script to automatically commit metadata changes:

```bash
#!/bin/bash
# check_metadata_changes.sh

# Extract metadata with change checking
python scripts/extract_metadata.py --all --check-changes

# Check if there are changes
if git diff --quiet metadata/; then
    echo "No metadata changes detected"
else
    echo "Metadata changes detected - committing to Git"
    git add metadata/
    git commit -m "Automated metadata update: $(date +%Y-%m-%d)"
    git push
fi
```

## Understanding Change Types

### Column Added
A new column was added to the table.

```
+ Column added: NEW_COLUMN (VARCHAR(100))
```

**Impact**: You may need to update your data pipeline to handle the new column.

### Column Removed
A column was removed from the table.

```
- Column removed: OLD_COLUMN (INTEGER)
```

**Impact**: Update your pipeline to remove references to this column. Existing data exports may fail.

### Type Changed
A column's data type was modified.

```
~ Type changed: AMOUNT
    NUMBER(18,2) → NUMERIC(18,2)
```

**Impact**: Verify that the new type is compatible with your PostgreSQL schema and data processing logic.

### Nullable Changed
A column's NULL constraint was modified.

```
~ Nullable changed: REQUIRED_FIELD → NOT NULL
```

**Impact**: If a column becomes NOT NULL, ensure your data pipeline handles this constraint.

### Position Changed
A column's position in the table changed.

```
~ Position changed: COLUMN_NAME
    Position 5 → Position 8
```

**Impact**: Usually minimal, but may affect SELECT * queries or position-based data processing.

## Best Practices

### 1. Regular Monitoring

Run metadata extraction with `--check-changes` regularly (daily or weekly) to catch schema changes early:

```bash
# Add to cron or Task Scheduler
python scripts/extract_metadata.py --all --check-changes
```

### 2. Review Changes Before Data Export

Always check for metadata changes before exporting data:

```bash
# Step 1: Check for metadata changes
python scripts/extract_metadata.py --all --check-changes

# Step 2: If changes detected, review them
# Step 3: Update PostgreSQL tables if needed
python scripts/create_tables.py --all --drop-existing

# Step 4: Proceed with data export
python scripts/export_data.py --all
```

### 3. Keep Change Logs

Preserve change logs in Git for audit trail:

```bash
git add metadata/changes/*.log
git commit -m "Update metadata change logs"
```

### 4. Document Breaking Changes

When significant changes occur, document them in your project:

```bash
# Create a migration note
echo "2024-12-28: Added RISK_RATING column to FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND" >> CHANGELOG.md
```

### 5. Test After Changes

After metadata changes are detected:

1. Update PostgreSQL schema
2. Test data export with small sample
3. Verify data import works correctly
4. Run full data pipeline

## Troubleshooting

### No Changes Detected But Schema Changed

If you know the schema changed but no changes are detected:

```bash
# Force re-extraction
python scripts/extract_metadata.py --all --check-changes --force
```

### Change Log Not Created

Ensure the `metadata/changes/` directory exists:

```bash
mkdir metadata\changes  # Windows
mkdir -p metadata/changes  # Unix/Mac
```

### Archived Files Not Created

Check that you have write permissions to the `metadata/` directory and that the original files exist.

### Comparison Errors

If metadata comparison fails, check the log files for detailed error messages. The system will continue processing other tables even if one fails.

## File Structure

```
metadata/
├── schemas/                                    # Current and archived metadata
│   ├── {table}_metadata.json                  # Current metadata
│   └── {table}_{YYYYMMDD}_metadata.json       # Archived versions
├── ddl/                                        # Current and archived DDL
│   ├── {table}_create.sql                     # Current DDL
│   └── {table}_{YYYYMMDD}_create.sql          # Archived versions
└── changes/                                    # Change logs
    └── {table}_changes.log                    # Persistent change history
```

## Related Documentation

- [Command Reference](command-reference.md) - Complete command reference
- [Complete Workflow](complete-workflow.md) - Full data pipeline workflow
- [Connection Management](connection-management.md) - Connection handling

## Summary

The metadata change tracking feature provides:
- ✅ Automatic change detection
- ✅ Detailed change alerts
- ✅ Versioned metadata history
- ✅ Persistent change logs
- ✅ Git-friendly workflow
- ✅ Minimal manual intervention

Use `--check-changes` flag regularly to stay informed about schema changes and maintain data pipeline integrity.
