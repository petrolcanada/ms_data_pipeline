# Metadata Change Tracking - Implementation Complete

## Overview

Implemented comprehensive metadata change tracking system that monitors Snowflake table schema changes, alerts users, archives old versions, and maintains change history.

## Implementation Date

December 28, 2024

## Components Implemented

### 1. Utility Modules

#### `pipeline/utils/metadata_comparator.py`
- `compare_metadata()` - Compares old vs new metadata
- Detects: column additions/removals, type changes, nullable changes, position changes, constraint changes
- `format_changes()` - Formats changes for display
- `_generate_summary()` - Creates human-readable summary

#### `pipeline/utils/change_logger.py`
- `log_change()` - Logs changes to `metadata/changes/{table}_changes.log`
- `log_initial_extraction()` - Logs first-time extraction
- `get_change_history()` - Retrieves change history
- `format_change_log()` - Formats log for display

### 2. Updated Metadata Extractor

#### `pipeline/extractors/metadata_extractor.py`

**New Methods:**
- `check_metadata_changed(table_name, new_metadata)` - Compares metadata and detects changes
- `archive_old_metadata(table_name)` - Archives old files with timestamp format `{table}_{YYYYMMDD}`

**Updated Methods:**
- `save_metadata_to_file()` - Now supports change checking and archiving
  - Returns tuple: `(metadata_file_path, comparison_result)`
  - Archives old files before saving new ones if changes detected
  - Logs changes to change history
- `extract_all_configured_tables()` - Added parameters:
  - `check_changes` - Enable change detection
  - `force` - Force re-extraction even if unchanged
  - Returns comparison results in response

**New Imports:**
- `MetadataComparator` from `pipeline.utils.metadata_comparator`
- `ChangeLogger` from `pipeline.utils.change_logger`
- `Optional`, `Tuple` from `typing`
- `datetime` from `datetime`

**New Instance Variables:**
- `self.ddl_dir` - Path to DDL directory
- `self.comparator` - MetadataComparator instance
- `self.change_logger` - ChangeLogger instance

### 3. Updated Extraction Script

#### `scripts/extract_metadata.py`

**New Command-Line Arguments:**
- `--check-changes` - Enable change detection and alerting
- `--force` - Force re-extraction even if no changes detected

**Enhanced Output:**
- Displays detailed change alerts when changes detected
- Shows change summary and detailed change list
- Indicates archived file locations
- Status indicators: `[NEW]`, `[CHANGED]`, `[UNCHANGED]`

**Alert Format:**
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

### 4. Directory Structure

Created `metadata/changes/` directory for change logs.

### 5. Documentation

#### `docs/metadata-change-tracking.md`
Comprehensive guide covering:
- Overview and quick start
- Feature descriptions
- Command reference
- Workflow examples
- Change type explanations
- Best practices
- Troubleshooting
- Git integration

#### Updated Documentation
- `docs/command-reference.md` - Added change tracking options
- `README.md` - Added metadata change tracking section with example

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

## Usage Examples

### Basic Change Detection
```bash
python scripts/extract_metadata.py --all --check-changes
```

### Force Re-extraction
```bash
python scripts/extract_metadata.py --all --check-changes --force
```

### With PostgreSQL Table Creation
```bash
python scripts/extract_metadata.py --all --check-changes --create-postgres
```

## Features

### Change Detection
- ✅ Column additions
- ✅ Column removals
- ✅ Data type changes
- ✅ Nullable constraint changes
- ✅ Column position changes
- ✅ Constraint modifications

### Automatic Archiving
- ✅ Archives old metadata with timestamp
- ✅ Archives old DDL with timestamp
- ✅ Naming format: `{table}_{YYYYMMDD}_metadata.json`
- ✅ Preserves current files

### Change Logging
- ✅ Persistent log files in `metadata/changes/`
- ✅ Timestamped entries
- ✅ Detailed change information
- ✅ Human-readable format

### User Alerts
- ✅ Prominent change alerts
- ✅ Detailed change breakdown
- ✅ Summary statistics
- ✅ Archived file locations

### Status Indicators
- ✅ `[NEW]` - First-time extraction
- ✅ `[CHANGED]` - Changes detected
- ✅ `[UNCHANGED]` - No changes

## Integration Points

### With Existing Workflow
1. Extract metadata with `--check-changes`
2. Review any detected changes
3. Update PostgreSQL tables if needed
4. Proceed with data export/import

### With Git
- Archived files can be committed to Git
- Change logs provide audit trail
- Supports version control of schema evolution

### Backward Compatibility
- ✅ Default behavior unchanged (no breaking changes)
- ✅ Change detection is opt-in via `--check-changes` flag
- ✅ Existing scripts continue to work without modification

## Testing Recommendations

### Manual Testing
1. Run initial extraction: `python scripts/extract_metadata.py --all --check-changes`
2. Verify `[NEW]` status for first-time tables
3. Run again without changes: verify `[UNCHANGED]` status
4. Manually modify a metadata file to simulate change
5. Run again: verify change detection and archiving

### Integration Testing
1. Test with real Snowflake schema changes
2. Verify archived files are created correctly
3. Verify change logs are written
4. Verify alerts display correctly

## Benefits

1. **Proactive Monitoring** - Catch schema changes early
2. **Audit Trail** - Complete history of all changes
3. **Version Control** - Timestamped archives of all versions
4. **Minimal Disruption** - Opt-in feature, doesn't affect existing workflows
5. **Clear Communication** - Detailed alerts explain exactly what changed
6. **Git-Friendly** - All files can be tracked in version control

## Future Enhancements (Optional)

- Email/Slack notifications for changes
- Automated Git commits on changes
- Schema diff visualization
- Change impact analysis
- Rollback capability
- Single table extraction support (currently only `--all` supported)

## Related Documentation

- [Metadata Change Tracking Guide](../../docs/metadata-change-tracking.md)
- [Command Reference](../../docs/command-reference.md)
- [Complete Workflow](../../docs/complete-workflow.md)

## Summary

The metadata change tracking feature is fully implemented and ready for use. It provides comprehensive monitoring of Snowflake table schema changes with automatic detection, archiving, logging, and user alerts. The feature integrates seamlessly with the existing workflow and is opt-in via the `--check-changes` flag.
