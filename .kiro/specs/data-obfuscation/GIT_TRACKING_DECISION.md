# Git Tracking Decision for Encrypted Files

## Decision

**Allow `.enc` files to be tracked by Git for transition purposes.**

---

## Rationale

### Why Allow `.enc` Files in Git?

1. **Security is Maintained**
   - Files are encrypted with AES-256-GCM
   - Password is never stored in Git
   - Safe to commit even to public repositories
   - Cannot be decrypted without password

2. **Transition Use Case**
   - User needs to transfer encrypted data between Snowflake and PostgreSQL servers
   - Git provides version control and easy transfer mechanism
   - Alternative to manual USB/network transfer
   - Enables automated deployment workflows

3. **Selective Tracking**
   - Export directories (`exports/`, `D:/snowflake_exports/`) are ignored
   - Prevents accidental commits of large directories
   - Individual `.enc` files can be added explicitly when needed
   - Gives user control over what gets tracked

4. **Temporary Files Protected**
   - `*.json.tmp` and `*.parquet.tmp` are ignored
   - Prevents accidental commits of unencrypted data
   - Automatic cleanup after operations

---

## Implementation

### .gitignore Configuration

```gitignore
# Data exports and imports (large files - don't commit to repo)
# Note: .enc files themselves are allowed for transition purposes
exports/
imports/
D:/snowflake_exports/
E:/postgres_imports/

# Temporary decryption files (never commit these)
*.json.tmp
*.parquet.tmp
*_decrypted.*
```

### What This Means

✅ **Allowed:**
- `*.enc` files (encrypted data, manifest, index)
- Can be committed to Git explicitly
- Safe for version control

❌ **Blocked:**
- Export/import directories (too large)
- Temporary unencrypted files (security risk)
- Decrypted data files (security risk)

---

## Usage Patterns

### Pattern 1: Git-Based Transfer (Recommended for Transition)

```bash
# Snowflake server
python scripts/export_data.py --all --obfuscate
cp -r D:/snowflake_exports/* ./encrypted_exports/
git add encrypted_exports/
git commit -m "Add encrypted exports"
git push

# PostgreSQL server
git pull
cp -r encrypted_exports/* E:/postgres_imports/
python scripts/import_data.py --all
```

### Pattern 2: Manual Transfer (Traditional)

```bash
# Snowflake server
python scripts/export_data.py --all --obfuscate
# Files in D:/snowflake_exports/ (ignored by Git)

# Manual transfer via USB/network
# Copy to PostgreSQL server

# PostgreSQL server
python scripts/import_data.py --all
```

---

## Security Guarantees

### What's Protected

1. **Encryption**
   - AES-256-GCM (industry standard)
   - PBKDF2 key derivation (100,000 iterations)
   - Unique salt per file
   - Authentication tag for integrity

2. **Password**
   - Never stored in Git
   - Never stored in files
   - User must remember/manage securely
   - Required for decryption

3. **Obfuscation**
   - Random folder names
   - Random file names
   - Encrypted manifest
   - Encrypted master index

### What Could Be Exposed

❌ **If committed to Git:**
- File sizes (visible in Git)
- Number of files (visible in Git)
- Commit timestamps (visible in Git)

✅ **Cannot be exposed:**
- File contents (encrypted)
- Table names (obfuscated + encrypted)
- Data structure (obfuscated + encrypted)
- Actual data (encrypted)

---

## Alternatives Considered

### Alternative 1: Block All `.enc` Files

**Pros:**
- Prevents any encrypted data in Git
- Smaller Git repository
- Forces manual transfer

**Cons:**
- ❌ No version control for exports
- ❌ Manual transfer required
- ❌ Harder to automate
- ❌ No transition support

**Decision:** Rejected - User needs transition support

### Alternative 2: Use Git LFS for `.enc` Files

**Pros:**
- Better handling of large files
- Doesn't bloat Git repo
- Still version controlled

**Cons:**
- Requires Git LFS setup
- Additional complexity
- May not be available on all systems

**Decision:** Optional - User can enable if needed

### Alternative 3: Separate Encrypted Exports Repo

**Pros:**
- Keeps main repo clean
- Dedicated space for exports
- Better organization

**Cons:**
- Additional repo to manage
- More complex workflow
- Overkill for transition use case

**Decision:** Rejected - Too complex for transition

---

## Recommendations

### For Transition Period

1. **Use Git for Transfer**
   - Commit encrypted exports to Git
   - Push to remote repository
   - Pull on PostgreSQL server
   - Import data

2. **Clean Up After Import**
   - Remove encrypted files from Git after successful import
   - Keep Git repo clean
   - Maintain only active exports

3. **Use Strong Passwords**
   - 16+ characters
   - Mixed case, numbers, symbols
   - Store in password manager

### For Production

1. **Consider Git LFS**
   - If many large exports
   - If frequent exports
   - If long-term storage needed

2. **Automate Cleanup**
   - Script to remove old exports
   - Keep only recent exports
   - Prevent repo bloat

3. **Monitor Repo Size**
   - Check Git repo size regularly
   - Clean up if too large
   - Use `git gc` to optimize

---

## Documentation

Created comprehensive guide:
- **`docs/git-tracking-strategy.md`** - Complete usage guide
- Explains what gets tracked
- Provides usage scenarios
- Includes security considerations
- Troubleshooting section

---

## Summary

**Decision:** Allow `.enc` files to be tracked by Git for transition purposes.

**Implementation:** 
- Export directories ignored
- `.enc` files allowed
- Temporary files blocked
- Comprehensive documentation provided

**Result:** User can use Git for encrypted data transfer while maintaining security and flexibility.

