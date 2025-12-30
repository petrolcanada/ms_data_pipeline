# Current State: Obfuscation and Encryption

## Date: December 28, 2024

## 📊 Summary Table

| Feature | export_data.py | extract_metadata.py | Default Behavior |
|---------|---------------|---------------------|------------------|
| **Obfuscation** | ✅ Implemented | ❌ Not Yet | **ON by default** |
| **Encryption** | ✅ Implemented | ❌ Not Yet | **Always ON** |
| **Password from .env** | ✅ Implemented | ❌ Not Yet | **Supported** |
| **--no-obfuscate flag** | ✅ Implemented | ❌ Not Yet | To disable obfuscation |

---

## 1. export_data.py (Data Export)

### ✅ Current State: FULLY IMPLEMENTED

#### Obfuscation
- **Default**: **ENABLED** (obfuscation is ON by default)
- **Control**: Use `--no-obfuscate` flag to disable
- **Logic**:
  ```python
  # Priority: --no-obfuscate flag > OBFUSCATE_NAMES env > default (True)
  if args.no_obfuscate:
      use_obfuscation = False
  else:
      use_obfuscation = getattr(settings, 'obfuscate_names', True)
  ```

#### Encryption
- **Default**: **ALWAYS ON** (cannot be disabled)
- **Algorithm**: AES-256-GCM with PBKDF2 (100,000 iterations)
- **Password Priority**:
  1. `--password-file` (if provided)
  2. `ENCRYPTION_PASSWORD` from `.env`
  3. Interactive prompt

#### Usage Examples

**With obfuscation (default):**
```bash
# Uses password from .env, obfuscation enabled
python scripts/export_data.py --all

# Output:
# 🔒 Name obfuscation: ENABLED
#    Folder and file names will be randomized
#    Master index will be created: index.enc
```

**Without obfuscation:**
```bash
# Uses password from .env, obfuscation disabled
python scripts/export_data.py --all --no-obfuscate

# Output:
# 📁 Name obfuscation: DISABLED
#    Using original table names for folders
```

**With password file:**
```bash
# Uses password from file, obfuscation enabled (default)
python scripts/export_data.py --all --password-file ~/.encryption_key
```

#### What Gets Obfuscated (when enabled)
- ✅ Folder names: `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND` → `a7f3d9e2c4b8f1a9`
- ✅ Data chunk files: `data_chunk_0.parquet.enc` → `b4c8f1a9.enc`
- ✅ Manifest files: `manifest.json` → `f7a2d8c4.enc`
- ✅ Master index: `index.enc` (maps obfuscated names to table names)

#### What's Always Encrypted
- ✅ All data chunks (`.enc` files)
- ✅ Manifest files (when obfuscated)
- ✅ Master index (when obfuscated)

---

## 2. extract_metadata.py (Metadata Extraction)

### ❌ Current State: NOT YET IMPLEMENTED

#### What Needs to Be Added

1. **Obfuscation Support**
   - Add `--no-obfuscate` flag
   - Default: **ENABLED** (same as export_data.py)
   - Obfuscate metadata JSON files
   - Obfuscate DDL SQL files
   - Create metadata master index

2. **Encryption Support**
   - Encrypt metadata JSON files
   - Encrypt DDL SQL files
   - Always ON (cannot be disabled)

3. **Password from .env**
   - Read `ENCRYPTION_PASSWORD` from `.env`
   - Same priority as export_data.py

#### Planned Usage (After Implementation)

**With obfuscation (default):**
```bash
# Uses password from .env, obfuscation enabled
python scripts/extract_metadata.py --all

# Expected output:
# 🔒 Metadata obfuscation: ENABLED
#    Metadata and DDL files will be encrypted and obfuscated
#    Master index will be created: metadata/index.enc
```

**Without obfuscation:**
```bash
# Uses password from .env, obfuscation disabled
python scripts/extract_metadata.py --all --no-obfuscate

# Expected output:
# 📁 Metadata obfuscation: DISABLED
#    Using original table names for metadata files
#    Files will still be encrypted but not obfuscated
```

#### What Will Be Obfuscated (when enabled)
- ⏳ Metadata JSON files: `{table}_metadata.json` → `a7f3d9e2.enc`
- ⏳ DDL SQL files: `{table}_create.sql` → `b4c8f1a9.enc`
- ⏳ Master index: `metadata/index.enc` (maps obfuscated names to table names)

#### What Will Always Be Encrypted
- ⏳ All metadata JSON files
- ⏳ All DDL SQL files
- ⏳ Master index (when obfuscated)

---

## 3. Configuration (.env)

### Current Settings

```bash
# Encryption password (used by both export and import)
ENCRYPTION_PASSWORD=your_secure_password_here

# Obfuscation (default: true)
OBFUSCATE_NAMES=true
```

### Behavior Matrix

| OBFUSCATE_NAMES | --no-obfuscate flag | Result |
|-----------------|---------------------|--------|
| `true` (default) | Not specified | **Obfuscation ON** |
| `true` | Specified | **Obfuscation OFF** |
| `false` | Not specified | **Obfuscation OFF** |
| `false` | Specified | **Obfuscation OFF** |

**Key Point**: The `--no-obfuscate` flag always wins (disables obfuscation regardless of .env setting)

---

## 4. Default Behavior Summary

### For Data Export (export_data.py) ✅

| Feature | Default | Can Disable? |
|---------|---------|--------------|
| Obfuscation | **ON** | Yes (`--no-obfuscate`) |
| Encryption | **ON** | No (always required) |
| Password from .env | **Supported** | Yes (use `--password-file` or prompt) |

### For Metadata Extraction (extract_metadata.py) ⏳

| Feature | Default (Planned) | Can Disable? |
|---------|-------------------|--------------|
| Obfuscation | **ON** | Yes (`--no-obfuscate`) |
| Encryption | **ON** | No (always required) |
| Password from .env | **Supported** | Yes (use `--password-file` or prompt) |

---

## 5. Quick Reference

### To Use Defaults (Obfuscation ON, Password from .env)

```bash
# Set password in .env
echo "ENCRYPTION_PASSWORD=MySecurePassword123" >> .env

# Export data (obfuscation enabled by default)
python scripts/export_data.py --all

# Extract metadata (obfuscation will be enabled by default after implementation)
python scripts/extract_metadata.py --all
```

### To Disable Obfuscation

```bash
# Export data without obfuscation
python scripts/export_data.py --all --no-obfuscate

# Extract metadata without obfuscation (after implementation)
python scripts/extract_metadata.py --all --no-obfuscate
```

### To Use Password File Instead of .env

```bash
# Create password file
echo "MySecurePassword123" > ~/.encryption_key
chmod 600 ~/.encryption_key

# Use password file
python scripts/export_data.py --all --password-file ~/.encryption_key
python scripts/extract_metadata.py --all --password-file ~/.encryption_key
```

---

## 6. Security Notes

### What's Secure by Default ✅
- ✅ All data is encrypted (AES-256-GCM)
- ✅ All file/folder names are obfuscated (by default)
- ✅ Password can be stored in `.env` (not in Git)
- ✅ Strong key derivation (100,000 PBKDF2 iterations)

### What You Need to Do 🔐
- 🔐 Set `ENCRYPTION_PASSWORD` in `.env` on both servers
- 🔐 Keep `.env` file secure (already in `.gitignore`)
- 🔐 Use a strong password (recommended: 20+ characters)
- 🔐 Never commit `.env` to Git

---

## Next Steps

1. ✅ **export_data.py** - Already supports obfuscation and encryption by default
2. ⏳ **extract_metadata.py** - Needs implementation (same pattern as export_data.py)
3. ⏳ **Documentation** - Update all guides to reflect default behavior
4. ⏳ **Testing** - Verify everything works end-to-end

---

## Answer to Your Question

**Q: Is obfuscation and encryption ON by default?**

**A: For export_data.py:**
- ✅ **Obfuscation**: **YES, ON by default** (use `--no-obfuscate` to disable)
- ✅ **Encryption**: **YES, ALWAYS ON** (cannot be disabled)

**A: For extract_metadata.py:**
- ⏳ **Obfuscation**: **Will be ON by default** (after implementation)
- ⏳ **Encryption**: **Will be ALWAYS ON** (after implementation)

**Both scripts will use the same password from `.env` automatically!**
