# Metadata Management Quick Reference

## Quick Start (Most Common Commands)

### Daily Workflow
```bash
# 1. Extract metadata with change tracking (uses .env password, change tracking enabled by default)
python scripts/extract_metadata.py --all

# 2. View changes for specific table (obfuscation enabled by default)
python scripts/view_change_history.py --table TABLE_NAME

# 3. Decrypt specific table for debugging
python scripts/decrypt_metadata.py --table TABLE_NAME

# 4. Clean up when done
python scripts/decrypt_metadata.py --clean
```

---

## Command Cheat Sheet

### Extract Metadata
| Command | Description |
|---------|-------------|
| `python scripts/extract_metadata.py --all` | Extract all tables (obfuscation & change tracking enabled by default) |
| `python scripts/extract_metadata.py --all --no-obfuscate` | Extract all tables (plain text, change tracking enabled) |
| `python scripts/extract_metadata.py --table TABLE_NAME` | Extract specific table |
| `python scripts/extract_metadata.py --all --force` | Force re-extraction even if no changes |
| `python scripts/extract_metadata.py --all --no-check-changes` | Disable change tracking |

### Decrypt Metadata
| Command | Description |
|---------|-------------|
| `python scripts/decrypt_metadata.py --all` | Decrypt all tables |
| `python scripts/decrypt_metadata.py --table TABLE_NAME` | Decrypt specific table |
| `python scripts/decrypt_metadata.py --list` | List available tables |
| `python scripts/decrypt_metadata.py --clean` | Delete all decrypted files |
| `python scripts/decrypt_metadata.py --all --password PWD` | Decrypt with explicit password |

### View Change History
| Command | Description |
|---------|-------------|
| `python scripts/view_change_history.py --table TABLE_NAME` | View all changes (obfuscation enabled by default) |
| `python scripts/view_change_history.py --table TABLE_NAME --no-obfuscate` | View all changes (plain text) |
| `python scripts/view_change_history.py --table TABLE_NAME --limit 5` | View last 5 changes |
| `python scripts/view_change_history.py --table TABLE_NAME --from 2024-01-01 --to 2024-01-31` | View changes in date range |
| `python scripts/view_change_history.py --summary` | View summary of all tables |

---

## File Locations

### Encrypted Mode (Obfuscation Enabled)
```
metadata/
├── schemas/
│   ├── {random_id}.enc              # Current encrypted metadata
│   └── {random_id}_{YYYYMMDD}.enc   # Archived versions
├── ddl/
│   ├── {random_id}.enc              # Current encrypted DDL
│   └── {random_id}_{YYYYMMDD}.enc   # Archived versions
├── changes/
│   └── {random_id}.enc              # Encrypted change logs
├── decrypted/                        # Temporary (NOT in Git)
│   ├── schemas/
│   │   └── TABLE_NAME_metadata.json
│   ├── ddl/
│   │   └── TABLE_NAME_create.sql
│   └── changes/
│       └── TABLE_NAME_changes.log
└── index.enc                         # Master index
```

### Plain Text Mode (Obfuscation Disabled)
```
metadata/
├── schemas/
│   ├── TABLE_NAME_metadata.json
│   └── TABLE_NAME_{YYYYMMDD}_metadata.json  # Archived
├── ddl/
│   ├── TABLE_NAME_create.sql
│   └── TABLE_NAME_{YYYYMMDD}_create.sql     # Archived
└── changes/
    └── TABLE_NAME_changes.log
```

---

## Environment Variables (.env)

```bash
# Required for Snowflake connection
SNOWFLAKE_USER=your_user
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_AUTH_METHOD=sso

# Required for encryption/obfuscation
ENCRYPTION_PASSWORD=your_strong_password
OBFUSCATE_NAMES=true
```

---

## Common Scenarios

### Scenario 1: First Time Setup
```bash
# 1. Configure .env file
cp .env.example .env
# Edit .env with your credentials

# 2. Configure tables
# Edit config/tables.yaml

# 3. Extract metadata (obfuscation & change tracking enabled by default)
python scripts/extract_metadata.py --all
```

### Scenario 2: Daily Check for Changes
```bash
# Extract and check for changes (enabled by default)
python scripts/extract_metadata.py --all

# If changes detected, review them
python scripts/view_change_history.py --summary
python scripts/view_change_history.py --table CHANGED_TABLE
```

### Scenario 3: Debug Specific Table
```bash
# 1. Decrypt the table
python scripts/decrypt_metadata.py --table TABLE_NAME

# 2. View files
cat metadata/decrypted/schemas/TABLE_NAME_metadata.json
cat metadata/decrypted/ddl/TABLE_NAME_create.sql
cat metadata/decrypted/changes/TABLE_NAME_changes.log

# 3. Clean up
python scripts/decrypt_metadata.py --clean
```

### Scenario 4: Review Historical Changes
```bash
# View all changes (obfuscation enabled by default)
python scripts/view_change_history.py --table TABLE_NAME

# View last week's changes
python scripts/view_change_history.py --table TABLE_NAME \
  --from 2024-01-01 --to 2024-01-07

# View last 10 changes
python scripts/view_change_history.py --table TABLE_NAME --limit 10

# View plain text change logs
python scripts/view_change_history.py --table TABLE_NAME --no-obfuscate
```

---

## Flags Reference

### extract_metadata.py
- `--all` - Extract all tables from config/tables.yaml
- `--table NAME` - Extract specific table
- `--no-obfuscate` - Disable encryption/obfuscation (enabled by default)
- `--no-check-changes` - Disable change detection (enabled by default)
- `--force` - Force re-extraction even if no changes
- `--create-postgres` - Create tables in PostgreSQL
- `--password-file PATH` - Path to file containing password

### decrypt_metadata.py
- `--all` - Decrypt all tables
- `--table NAME` - Decrypt specific table
- `--list` - List available tables
- `--clean` - Delete all decrypted files
- `--password PWD` - Explicit password (optional, uses .env)
- `--show-changes` - Display change history after decrypting

### view_change_history.py
- `--table NAME` - View changes for specific table
- `--no-obfuscate` - Change logs are NOT encrypted (obfuscation enabled by default)
- `--limit N` - Show last N changes
- `--from DATE` - Start date (YYYY-MM-DD)
- `--to DATE` - End date (YYYY-MM-DD)
- `--summary` - Show summary of all tables
- `--password PWD` - Explicit password (optional, uses .env)

---

## Troubleshooting Quick Fixes

| Problem | Solution |
|---------|----------|
| Password prompt appears | Add `ENCRYPTION_PASSWORD` to `.env` |
| Cannot decrypt files | Run `python scripts/decrypt_metadata.py --list` to verify table names |
| Change logs not showing | Change tracking is enabled by default; check if extraction ran successfully |
| Decrypted files in Git | Run `python scripts/decrypt_metadata.py --clean` |
| Wrong password error | Check `.env` file or use `--password` flag |
| Table not found | Check `config/tables.yaml` and verify table name (case-sensitive) |

---

## Git Best Practices

### ✅ DO Commit
- `metadata/schemas/*.enc` - Encrypted metadata
- `metadata/ddl/*.enc` - Encrypted DDL
- `metadata/changes/*.enc` - Encrypted change logs
- `metadata/index.enc` - Master index
- `metadata/schemas/*_{YYYYMMDD}_*.enc` - Archived versions
- `config/tables.yaml` - Table configuration

### ❌ DON'T Commit
- `metadata/decrypted/*` - Temporary decrypted files
- `.env` - Contains passwords
- `*.tmp` - Temporary files

### Verify .gitignore
```bash
grep -E "(metadata/decrypted|\.env)" .gitignore
```

---

## Password Priority

Scripts use passwords in this order:
1. **Command line argument**: `--password mypassword`
2. **Environment variable**: `ENCRYPTION_PASSWORD` in `.env`
3. **Secure prompt**: Interactive password input

**Recommendation**: Use `.env` file for convenience and security.

---

## Integration Points

### With Data Export/Import
```bash
# Same password for all operations
ENCRYPTION_PASSWORD=your_password

# Extract metadata
python scripts/extract_metadata.py --all --obfuscate --check-changes

# Export data
python scripts/export_data.py --all

# Import data
python scripts/import_data.py --all
```

### With Git Workflow
```bash
# 1. Extract metadata (obfuscation & change tracking enabled by default)
python scripts/extract_metadata.py --all

# 2. Review changes
python scripts/view_change_history.py --summary

# 3. Commit encrypted files
git add metadata/schemas/*.enc metadata/ddl/*.enc metadata/changes/*.enc
git commit -m "Update metadata: 2 columns added to FINANCIAL_DATA"
git push
```

---

## Performance Tips

1. **Use `--all` for batch operations** instead of individual tables
2. **Skip `--force`** unless necessary (faster when no changes)
3. **Clean up decrypted files** regularly to save disk space
4. **Use `--limit`** when viewing change history for faster results

---

## Security Reminders

- 🔒 Never commit `.env` file
- 🔒 Never commit `metadata/decrypted/` folder
- 🔒 Use strong passwords (min 16 characters)
- 🔒 Clean up decrypted files after viewing
- 🔒 Encrypted files are safe to commit to Git
- 🔒 Change logs contain table names but no sensitive data

---

## Need More Help?

- **Full Workflow Guide**: `docs/metadata-workflow-guide.md`
- **Metadata Directory README**: `metadata/README.md`
- **Change Logs README**: `metadata/changes/README.md`
- **Decrypted Files README**: `metadata/decrypted/README.md`
