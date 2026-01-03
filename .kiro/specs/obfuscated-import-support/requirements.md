# Requirements: Obfuscated Import Support

## Problem Statement

When data is exported with name obfuscation enabled, folder names are deterministic hashes (e.g., `a3f2b9c1...`) instead of human-readable table names. The current `import_data.py` script expects human-readable folder names and fails to import obfuscated data.

**Current Behavior:**
```
Export creates: D:/snowflake_exports/a3f2b9c1d4e5f6a7/  (obfuscated)
Import expects: C:/ms_dataset_init/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/  (human-readable)
Result: ❌ Import directory not found
```

## Solution Overview

The import script needs to:
1. Detect if folders are obfuscated (hash-based names)
2. Map obfuscated folder IDs back to table names
3. Support both obfuscated and non-obfuscated imports

## Glossary

- **Obfuscated_Folder**: A folder named with a deterministic hash instead of the table name
- **Folder_ID**: The hash-based identifier for an obfuscated folder
- **Manifest**: JSON file containing export metadata, including table name
- **DataObfuscator**: Component that generates deterministic folder IDs from table names

## Requirements

### Requirement 1: Detect Obfuscation

**User Story:** As a data engineer, I want the import script to automatically detect if folders are obfuscated, so that I don't need to specify this manually.

#### Acceptance Criteria

1. WHEN scanning the import directory, THE Import_Script SHALL check if folder names match the obfuscation pattern (32-character hex strings)
2. WHEN a folder name is a 32-character hex string, THE Import_Script SHALL treat it as an obfuscated folder
3. WHEN a folder name matches a table name from config, THE Import_Script SHALL treat it as a non-obfuscated folder
4. THE Import_Script SHALL support mixed scenarios (some obfuscated, some not)

### Requirement 2: Map Obfuscated Folders to Table Names

**User Story:** As a data engineer, I want the import script to identify which obfuscated folder corresponds to which table, so that data is loaded into the correct PostgreSQL tables.

#### Acceptance Criteria

1. WHEN an obfuscated folder is found, THE Import_Script SHALL decrypt and read the manifest file
2. WHEN the manifest is read, THE Import_Script SHALL extract the table_name field
3. WHEN the table name is extracted, THE Import_Script SHALL match it to the configuration in tables.yaml
4. IF the manifest is encrypted, THE Import_Script SHALL decrypt it using the provided password
5. IF the manifest cannot be decrypted or read, THE Import_Script SHALL log an error and skip that folder

### Requirement 3: Generate Folder ID Mapping

**User Story:** As a data engineer, I want to see a mapping of obfuscated folder IDs to table names, so that I can verify the correct folders are being imported.

#### Acceptance Criteria

1. WHEN importing with obfuscation, THE Import_Script SHALL display a mapping table showing folder ID → table name
2. THE Import_Script SHALL use the same deterministic hash algorithm as the export script
3. WHEN a table is configured, THE Import_Script SHALL generate its expected folder ID
4. THE Import_Script SHALL verify that the expected folder ID exists in the import directory

### Requirement 4: Support Both Obfuscated and Non-Obfuscated Imports

**User Story:** As a data engineer, I want the import script to work with both obfuscated and non-obfuscated exports, so that I have flexibility in my workflow.

#### Acceptance Criteria

1. WHEN importing a non-obfuscated export, THE Import_Script SHALL use the table name as the folder name
2. WHEN importing an obfuscated export, THE Import_Script SHALL use the folder ID as the folder name
3. THE Import_Script SHALL automatically detect which mode to use based on folder names
4. THE Import_Script SHALL support the --obfuscated flag to force obfuscation mode

### Requirement 5: Handle Encrypted Manifests

**User Story:** As a data engineer, I want the import script to decrypt encrypted manifest files, so that I can read the table name from obfuscated exports.

#### Acceptance Criteria

1. WHEN a manifest.json file is not found, THE Import_Script SHALL look for encrypted manifest files (*.enc)
2. WHEN an encrypted manifest is found, THE Import_Script SHALL decrypt it using the provided password
3. WHEN decryption succeeds, THE Import_Script SHALL parse the JSON and extract the table name
4. WHEN decryption fails, THE Import_Script SHALL log an error with the folder ID and skip that folder
5. THE Import_Script SHALL cache decrypted manifests to avoid repeated decryption

### Requirement 6: Provide Discovery Mode

**User Story:** As a data engineer, I want to scan the import directory and see what tables are available, so that I know what can be imported.

#### Acceptance Criteria

1. WHEN the --discover flag is provided, THE Import_Script SHALL scan the import directory
2. FOR EACH folder found, THE Import_Script SHALL attempt to read the manifest
3. THE Import_Script SHALL display a table showing: folder name, table name, row count, export date
4. THE Import_Script SHALL indicate if a folder is obfuscated or not
5. THE Import_Script SHALL not import any data in discovery mode

### Requirement 7: Error Handling for Missing Folders

**User Story:** As a data engineer, I want clear error messages when expected folders are missing, so that I can troubleshoot import issues.

#### Acceptance Criteria

1. WHEN a table is configured but its folder is not found, THE Import_Script SHALL log a clear error message
2. THE error message SHALL include: expected folder name/ID, import directory path, table name
3. IF obfuscation is detected, THE error message SHALL show the expected folder ID
4. THE Import_Script SHALL continue importing other tables instead of failing completely
5. THE Import_Script SHALL provide a summary of successful and failed imports at the end

## Non-Functional Requirements

### Performance
- Manifest decryption should be cached to avoid repeated decryption
- Folder scanning should be efficient for directories with many folders

### Usability
- The script should work without additional flags in most cases (auto-detect)
- Error messages should be clear and actionable
- Progress should be displayed for long-running operations

### Compatibility
- Must work with existing non-obfuscated exports
- Must work with obfuscated exports created by export_data.py
- Must use the same obfuscation algorithm as export_data.py
