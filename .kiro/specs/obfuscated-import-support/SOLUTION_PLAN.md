# Solution Plan: Obfuscated Import Support

## Problem Summary

Your exported data uses obfuscated folder names (e.g., `a3f2b9c1d4e5f6a7`), but the import script expects human-readable table names (e.g., `FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND`).

**Current State:**
```
Import Directory: C:\Users\lzhyx\local_scripts\ms_dataset_init\
├── a3f2b9c1d4e5f6a7/  ← Obfuscated folder (what table is this?)
├── b8e4f1a2c3d5e6f7/  ← Obfuscated folder
└── c9f5a3b4d6e7f8a9/  ← Obfuscated folder

Import script looks for:
├── FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/  ← Not found!
├── FUND_PERFORMANCE_HISTORY/
└── FUND_HOLDINGS/
```

---

## Solution Approaches

### Option 1: Auto-Discovery (Recommended) ⭐

**How it works:**
1. Scan import directory for all folders
2. For each folder, decrypt and read the manifest
3. Extract the table name from the manifest
4. Match table name to configuration
5. Import data

**Pros:**
- ✅ Fully automatic - no manual mapping needed
- ✅ Works with any obfuscated export
- ✅ No additional files needed
- ✅ Handles mixed obfuscated/non-obfuscated folders

**Cons:**
- ⚠️ Requires decrypting manifests (slower initial scan)
- ⚠️ Needs password to discover tables

**Implementation:**
- Modify `import_data.py` to scan and discover folders
- Add `--discover` flag to show available tables
- Auto-detect obfuscation based on folder names

---

### Option 2: Deterministic Folder ID Matching

**How it works:**
1. For each table in `tables.yaml`, generate its expected folder ID
2. Look for that folder ID in the import directory
3. Import from the matched folder

**Pros:**
- ✅ Fast - no manifest decryption needed for discovery
- ✅ Deterministic - same table always gets same folder ID
- ✅ No password needed for discovery

**Cons:**
- ⚠️ Requires tables.yaml to be accurate
- ⚠️ Can't handle manually moved/renamed folders

**Implementation:**
- Use `DataObfuscator.generate_folder_id(table_name)` to compute expected folder ID
- Match against actual folders in import directory

---

### Option 3: Index File (Not Recommended)

**How it works:**
1. Export creates an index file mapping folder IDs to table names
2. Import reads the index file to know which folder is which

**Pros:**
- ✅ Fast lookup
- ✅ No manifest decryption needed

**Cons:**
- ❌ Index file is a security risk (reveals table names)
- ❌ Defeats the purpose of obfuscation
- ❌ Extra file to manage and transfer

**Not recommended** - defeats the purpose of obfuscation!

---

## Recommended Solution: Hybrid Approach

Combine Option 1 and Option 2 for best results:

### Phase 1: Quick Match (Deterministic)
1. For each table in `tables.yaml`, generate expected folder ID
2. Check if that folder exists in import directory
3. If found, import from that folder

### Phase 2: Discovery (Fallback)
1. If folder not found by deterministic match, scan all folders
2. Decrypt manifests to find table names
3. Match to configuration and import

### Phase 3: Manual Discovery
1. Provide `--discover` flag to scan and show all available tables
2. Display mapping: folder ID → table name → row count

---

## Implementation Plan

### Step 1: Add Discovery Function

```python
def discover_tables(import_base_dir: str, password: str) -> Dict[str, str]:
    """
    Discover tables in import directory
    
    Returns:
        Dictionary mapping folder_id → table_name
    """
    import_dir = Path(import_base_dir)
    discovered = {}
    
    for folder in import_dir.iterdir():
        if not folder.is_dir():
            continue
        
        # Try to read manifest
        manifest_file = folder / "manifest.json"
        
        # If not found, look for encrypted manifest
        if not manifest_file.exists():
            enc_manifests = list(folder.glob("*.enc"))
            if enc_manifests:
                # Decrypt first .enc file found
                manifest_file = decrypt_manifest(enc_manifests[0], password)
        
        if manifest_file and manifest_file.exists():
            with open(manifest_file, 'r') as f:
                manifest = json.load(f)
                table_name = manifest.get('table_name')
                if table_name:
                    discovered[folder.name] = table_name
    
    return discovered
```

### Step 2: Add Deterministic Matching

```python
def find_table_folder(table_name: str, import_base_dir: str, obfuscator: DataObfuscator) -> Path:
    """
    Find folder for table (obfuscated or not)
    
    Returns:
        Path to folder, or None if not found
    """
    import_dir = Path(import_base_dir)
    
    # Try human-readable name first
    folder = import_dir / table_name
    if folder.exists():
        return folder
    
    # Try obfuscated folder ID
    folder_id = obfuscator.generate_folder_id(table_name)
    folder = import_dir / folder_id
    if folder.exists():
        return folder
    
    return None
```

### Step 3: Update import_table Function

```python
def import_table(
    table_config: dict,
    password: str,
    import_base_dir: str,
    obfuscator: DataObfuscator = None,
    truncate_first: bool = False,
    keep_decrypted: bool = False
):
    table_name = table_config['name']
    
    # Find folder (obfuscated or not)
    if obfuscator:
        import_dir = find_table_folder(table_name, import_base_dir, obfuscator)
    else:
        import_dir = Path(import_base_dir) / table_name
    
    if not import_dir or not import_dir.exists():
        raise FileNotFoundError(f"Import directory not found for table: {table_name}")
    
    print(f"📁 Import folder: {import_dir.name}")
    if obfuscator and import_dir.name != table_name:
        print(f"   (Obfuscated folder ID for {table_name})")
    
    # Rest of import logic...
```

### Step 4: Add --discover Flag

```python
parser.add_argument(
    "--discover",
    action="store_true",
    help="Discover and list available tables without importing"
)

# In main():
if args.discover:
    print("\n🔍 Discovering tables in import directory...")
    discovered = discover_tables(import_base_dir, password)
    
    print(f"\n{'Folder ID':<35} {'Table Name':<50} {'Status'}")
    print("-" * 100)
    
    for folder_id, table_name in discovered.items():
        # Check if in config
        in_config = any(t['name'] == table_name for t in config['tables'])
        status = "✅ In config" if in_config else "⚠️  Not in config"
        print(f"{folder_id:<35} {table_name:<50} {status}")
    
    sys.exit(0)
```

---

## Usage Examples

### Discover Available Tables:
```bash
python scripts/import_data.py --discover
```

**Output:**
```
🔍 Discovering tables in import directory...

Folder ID                            Table Name                                          Status
----------------------------------------------------------------------------------------------------
a3f2b9c1d4e5f6a7b8c9d0e1f2a3b4c5    FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND             ✅ In config
b8e4f1a2c3d5e6f7a8b9c0d1e2f3a4b5    FUND_PERFORMANCE_HISTORY                           ✅ In config
c9f5a3b4d6e7f8a9b0c1d2e3f4a5b6c7    FUND_HOLDINGS                                      ✅ In config
```

### Import Single Table (Auto-Detect):
```bash
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```

The script will:
1. Generate expected folder ID for the table
2. Look for that folder in import directory
3. Import from the obfuscated folder

### Import All Tables:
```bash
python scripts/import_data.py --all
```

The script will:
1. For each table in config, find its folder (obfuscated or not)
2. Import data from each folder

---

## Migration Path

### For Existing Users:

**If you have non-obfuscated exports:**
- ✅ No changes needed - script will auto-detect

**If you have obfuscated exports:**
- ✅ Script will auto-detect and use folder IDs
- ✅ Use `--discover` to see what's available

**If you have mixed exports:**
- ✅ Script handles both automatically

---

## Testing Plan

### Test Case 1: Non-Obfuscated Import
```bash
# Export without obfuscation
python scripts/export_data.py --table test_table --no-obfuscate

# Import should work as before
python scripts/import_data.py --table test_table
```

### Test Case 2: Obfuscated Import
```bash
# Export with obfuscation (default)
python scripts/export_data.py --table test_table

# Import should auto-detect obfuscation
python scripts/import_data.py --table test_table
```

### Test Case 3: Discovery
```bash
# Discover all tables
python scripts/import_data.py --discover
```

### Test Case 4: Mixed Folders
```bash
# Some obfuscated, some not
python scripts/import_data.py --all
```

---

## Implementation Checklist

- [ ] Add `DataObfuscator` import to `import_data.py`
- [ ] Add `discover_tables()` function
- [ ] Add `find_table_folder()` function
- [ ] Update `import_table()` to use `find_table_folder()`
- [ ] Add `--discover` flag
- [ ] Add obfuscation detection logic
- [ ] Update error messages to show folder IDs
- [ ] Add tests for obfuscated imports
- [ ] Update documentation

---

## Estimated Effort

- **Implementation:** 2-3 hours
- **Testing:** 1 hour
- **Documentation:** 30 minutes
- **Total:** 3.5-4.5 hours

---

## Next Steps

1. **Review this plan** - Does it solve your problem?
2. **Choose approach** - Hybrid (recommended) or another?
3. **Implement** - I can implement this now if you approve
4. **Test** - Test with your actual obfuscated exports
5. **Deploy** - Use for all future imports

---

## Questions?

- Do you want me to implement this now?
- Do you prefer a different approach?
- Any specific requirements I missed?
