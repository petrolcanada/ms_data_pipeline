# Metadata Change Tracking - Quick Reference

## Commands

```bash
# Check for metadata changes
python scripts/extract_metadata.py --all --check-changes

# Force re-extraction even if unchanged
python scripts/extract_metadata.py --all --check-changes --force

# Check changes and create PostgreSQL tables
python scripts/extract_metadata.py --all --check-changes --create-postgres
```

## What Gets Tracked

| Change Type | Description | Example |
|-------------|-------------|---------|
| Column Added | New column in table | `+ Column added: RISK_RATING (VARCHAR(50))` |
| Column Removed | Column deleted from table | `- Column removed: OLD_FIELD (INTEGER)` |
| Type Changed | Data type modified | `~ Type changed: AMOUNT (NUMBER → NUMERIC)` |
| Nullable Changed | NULL constraint changed | `~ Nullable changed: FIELD → NOT NULL` |
| Position Changed | Column order changed | `~ Position changed: COL (5 → 8)` |

## File Locations

```
metadata/
├── schemas/
│   ├── {table}_metadata.json                  # Current
│   └── {table}_{YYYYMMDD}_metadata.json       # Archived
├── ddl/
│   ├── {table}_create.sql                     # Current
│   └── {table}_{YYYYMMDD}_create.sql          # Archived
└── changes/
    └── {table}_changes.log                    # Change history
```

## Status Indicators

- `[NEW]` - First-time extraction
- `[CHANGED]` - Changes detected and archived
- `[UNCHANGED]` - No changes detected

## Workflow

1. **Daily Check**: Run `--check-changes` to monitor for schema changes
2. **Review Changes**: If changes detected, review the alert details
3. **Update Tables**: Update PostgreSQL schema if needed
4. **Commit to Git**: Add archived files to version control
5. **Proceed**: Continue with data export/import

## Example Output

### No Changes
```
✓ No metadata changes detected for any tables

✓ [UNCHANGED] FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
  Columns: 45
  Rows: 50,000
```

### Changes Detected
```
⚠️  METADATA CHANGES DETECTED!

Table: FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
Summary: 1 column added

Detailed Changes:
  + Column added: RISK_RATING (VARCHAR(50))

Archived old metadata:
  • metadata/schemas/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_metadata.json
  • metadata/ddl/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND_20241228_create.sql
```

## Best Practices

1. ✅ Run `--check-changes` regularly (daily/weekly)
2. ✅ Review changes before data export
3. ✅ Commit archived files to Git
4. ✅ Update PostgreSQL tables when schema changes
5. ✅ Test data pipeline after schema changes

## Troubleshooting

| Issue | Solution |
|-------|----------|
| No changes detected but schema changed | Use `--force` flag |
| Change log not created | Ensure `metadata/changes/` directory exists |
| Archived files not created | Check write permissions on `metadata/` directory |

## Related Commands

```bash
# View change log
type metadata\changes\{table}_changes.log

# List archived metadata
dir metadata\schemas\*_????????_metadata.json

# List archived DDL
dir metadata\ddl\*_????????_create.sql

# Add to Git
git add metadata/
git commit -m "Metadata change: {description}"
```
