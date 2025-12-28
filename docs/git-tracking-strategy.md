# Git Tracking Strategy for Encrypted Files

## Overview

This document explains how encrypted files (`.enc`) are handled in Git for transition purposes.

---

## What Gets Tracked

### ✅ **Tracked by Git**

**Encrypted files (`.enc`)** - Allowed for transition purposes:
- `*.enc` files can be committed to Git
- Useful for transferring encrypted data between systems
- Safe to commit (encrypted with strong password)
- Examples:
  - `index.enc` - Master index
  - `a7f3d9e2c4b8f1a9.enc` - Encrypted data chunks
  - `f7a2d8c4.enc` - Encrypted manifest files

**Why allow `.enc` files?**
- Encrypted data is safe to store in Git (password-protected)
- Enables version control of encrypted exports
- Facilitates transition between Snowflake and PostgreSQL servers
- Can be used as backup mechanism

---

## What Gets Ignored

### ❌ **Ignored by Git**

**1. Export/Import Directories:**
```gitignore
exports/
imports/
D:/snowflake_exports/
E:/postgres_imports/
```
- Large directories with many files
- Prevents accidental commits of entire export folders
- Individual `.enc` files can still be added explicitly if needed

**2. Temporary Decryption Files:**
```gitignore
*.json.tmp
*.parquet.tmp
*_decrypted.*
```
- Temporary files created during encryption/decryption
- Should never be committed (unencrypted data)
- Automatically cleaned up after operations

**3. Environment Variables:**
```gitignore
.env
.env.local
.env.*.local
```
- Contains passwords and credentials
- **NEVER** commit these files

---

## Usage Scenarios

### Scenario 1: Track Encrypted Exports for Transition

**Use Case:** You want to commit encrypted exports to Git for transfer between servers.

```bash
# Export with obfuscation
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND --obfuscate

# Encrypted files are created
# D:/snowflake_exports/a7f3d9e2c4b8f1a9/*.enc
# D:/snowflake_exports/index.enc

# Copy to repo for Git tracking
cp D:/snowflake_exports/index.enc ./encrypted_exports/
cp -r D:/snowflake_exports/a7f3d9e2c4b8f1a9 ./encrypted_exports/

# Add to Git
git add encrypted_exports/
git commit -m "Add encrypted export for transition"
git push

# On PostgreSQL server
git pull
cp -r encrypted_exports/* E:/postgres_imports/
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

**Benefits:**
- ✅ Encrypted data safe in Git
- ✅ Version controlled
- ✅ Easy transfer between servers
- ✅ No manual USB/network transfer needed

---

### Scenario 2: Keep Exports Outside Git (Default)

**Use Case:** You prefer manual transfer and don't want exports in Git.

```bash
# Export directly to export directory
python scripts/export_data.py --all --obfuscate

# Files created in D:/snowflake_exports/
# This directory is ignored by Git

# Manual transfer via USB/network
# Copy D:/snowflake_exports/ to E:/postgres_imports/

# Import on PostgreSQL server
python scripts/import_data.py --all
```

**Benefits:**
- ✅ No Git repo bloat
- ✅ Faster Git operations
- ✅ Traditional offline transfer workflow

---

## Security Considerations

### Safe to Commit

✅ **Encrypted files (`.enc`):**
- Protected by AES-256-GCM encryption
- Password not stored anywhere
- Safe to commit to Git (even public repos)
- Cannot be decrypted without password

### NEVER Commit

❌ **Unencrypted files:**
- `.env` files (contain passwords)
- `*.json.tmp` (temporary unencrypted data)
- `*.parquet.tmp` (temporary unencrypted data)
- `*_decrypted.*` (decrypted data files)

❌ **Passwords:**
- Never commit passwords to Git
- Never commit password files
- Use password managers or secure vaults

---

## Best Practices

### 1. **Use Strong Passwords**
```bash
# Good password (16+ characters, mixed)
MyS3cur3P@ssw0rd!2024

# Bad password
password123
```

### 2. **Separate Encrypted Exports Directory**
```bash
# Create dedicated directory for Git-tracked exports
mkdir encrypted_exports/

# Export to this directory
python scripts/export_data.py --all --obfuscate
cp -r D:/snowflake_exports/* encrypted_exports/

# Add to Git
git add encrypted_exports/
```

### 3. **Use .gitattributes for Large Files**

If you have many large `.enc` files, consider using Git LFS:

```bash
# .gitattributes
*.enc filter=lfs diff=lfs merge=lfs -text
```

### 4. **Clean Up After Import**
```bash
# After successful import, remove from Git if no longer needed
git rm -r encrypted_exports/
git commit -m "Remove imported data"
```

---

## Git Commands Reference

### Check What Will Be Committed
```bash
# See what files are staged
git status

# See what files are ignored
git status --ignored

# Check if .enc files are tracked
git ls-files "*.enc"
```

### Add Encrypted Files
```bash
# Add specific encrypted file
git add encrypted_exports/index.enc

# Add all encrypted files in directory
git add encrypted_exports/

# Force add even if in ignored directory
git add -f D:/snowflake_exports/index.enc
```

### Remove Encrypted Files
```bash
# Remove from Git but keep locally
git rm --cached encrypted_exports/*.enc

# Remove from Git and delete locally
git rm encrypted_exports/*.enc
```

---

## Troubleshooting

### Issue: `.enc` files not being tracked

**Problem:** Git ignores `.enc` files even though they should be tracked.

**Solution:** Check if parent directory is ignored:
```bash
# Check .gitignore
cat .gitignore | grep exports

# If exports/ is ignored, move files out
mv D:/snowflake_exports/*.enc ./encrypted_exports/
git add encrypted_exports/
```

### Issue: Accidentally committed unencrypted data

**Problem:** Committed `.json` or `.parquet` files without encryption.

**Solution:** Remove from Git history:
```bash
# Remove from current commit
git rm --cached sensitive_file.json
git commit --amend

# Remove from history (if already pushed)
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch sensitive_file.json" \
  --prune-empty --tag-name-filter cat -- --all
```

### Issue: Git repo too large

**Problem:** Too many `.enc` files making repo slow.

**Solution:** Use Git LFS or clean up old exports:
```bash
# Install Git LFS
git lfs install

# Track .enc files with LFS
git lfs track "*.enc"
git add .gitattributes
git commit -m "Track .enc files with LFS"

# Or remove old exports
git rm encrypted_exports/old_export_*
git commit -m "Clean up old exports"
```

---

## Summary

| File Type | Tracked by Git? | Reason |
|-----------|----------------|---------|
| `*.enc` | ✅ Yes | Encrypted, safe to commit |
| `index.enc` | ✅ Yes | Encrypted master index |
| `exports/` | ❌ No | Directory ignored (too large) |
| `*.json.tmp` | ❌ No | Temporary unencrypted files |
| `*.parquet.tmp` | ❌ No | Temporary unencrypted files |
| `.env` | ❌ No | Contains passwords |
| `*_decrypted.*` | ❌ No | Unencrypted data |

**Key Takeaway:** Encrypted files (`.enc`) are safe to track in Git for transition purposes, but export directories are ignored by default to prevent repo bloat. You can explicitly add encrypted files when needed.

