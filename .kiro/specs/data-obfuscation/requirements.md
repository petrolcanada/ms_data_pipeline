# Requirements Document: Data Export Obfuscation

## Introduction

This feature adds name obfuscation to exported data files to enhance security by making folder and file names non-identifiable. This prevents casual observation from revealing sensitive table names or data structure.

## Glossary

- **Export_System**: The data export pipeline that extracts data from Snowflake
- **Obfuscation_Module**: Component that generates random identifiers for folders and files
- **Master_Index**: Encrypted file that maps obfuscated identifiers to real table names
- **Manifest**: JSON file inside each export folder containing chunk metadata and mappings
- **Import_System**: The data import pipeline that loads data into PostgreSQL

## Requirements

### Requirement 1: Obfuscated Folder Names

**User Story:** As a security-conscious user, I want exported data folders to use random identifiers instead of table names, so that casual observers cannot identify which tables are being transferred.

#### Acceptance Criteria

1. WHEN exporting a table, THE Export_System SHALL generate a random unique identifier for the folder name
2. THE Export_System SHALL use hexadecimal format identifiers (e.g., "a7f3d9e2c4b8f1a9")
3. THE Export_System SHALL ensure folder identifiers are unique within an export session
4. THE Export_System SHALL store the mapping between folder identifier and table name in the Master_Index

### Requirement 2: Obfuscated File Names

**User Story:** As a security-conscious user, I want data chunk files to use random names instead of sequential names, so that file listings don't reveal data structure or chunk count.

#### Acceptance Criteria

1. WHEN saving a data chunk, THE Export_System SHALL generate a random unique identifier for the file name
2. THE Export_System SHALL use hexadecimal format with .enc extension (e.g., "b4c8f1a9.enc")
3. THE Export_System SHALL ensure file identifiers are unique within a folder
4. THE Export_System SHALL store the mapping between file identifier and chunk metadata in the Manifest

### Requirement 3: Encrypted Master Index

**User Story:** As a security-conscious user, I want the master index file to be encrypted, so that the mapping between obfuscated names and real table names is protected.

#### Acceptance Criteria

1. WHEN export completes, THE Export_System SHALL create a master index file named ".export_index.enc"
2. THE Export_System SHALL encrypt the master index using the same password as data files
3. THE Export_System SHALL include table name, folder identifier, and export timestamp in the index
4. THE Master_Index SHALL use JSON format before encryption
5. THE Export_System SHALL save the master index in the export base directory

### Requirement 4: Manifest File Mapping

**User Story:** As a developer, I want each export folder to contain a manifest with file mappings, so that the import process can reconstruct the correct chunk order.

#### Acceptance Criteria

1. WHEN creating the manifest, THE Export_System SHALL include a "file_mappings" section
2. THE Manifest SHALL map each obfuscated file name to its chunk number and metadata
3. THE Manifest SHALL include the real table name for verification during import
4. THE Manifest SHALL remain unencrypted (contains no sensitive data beyond what's already in folder)

### Requirement 5: Import System Compatibility

**User Story:** As a user, I want the import system to automatically handle obfuscated exports, so that I can import data without manual intervention.

#### Acceptance Criteria

1. WHEN importing data, THE Import_System SHALL detect if export uses obfuscation
2. THE Import_System SHALL decrypt the master index to find the correct folder
3. THE Import_System SHALL read the manifest to determine correct chunk order
4. THE Import_System SHALL process chunks in the correct order regardless of file names
5. THE Import_System SHALL verify table name matches between index and manifest

### Requirement 6: Configuration Control

**User Story:** As a user, I want to enable or disable obfuscation via configuration, so that I can choose the appropriate security level for my use case.

#### Acceptance Criteria

1. THE Export_System SHALL support an "obfuscate_names" configuration option
2. WHEN obfuscate_names is true, THE Export_System SHALL use obfuscated names
3. WHEN obfuscate_names is false, THE Export_System SHALL use original table names (backward compatible)
4. THE configuration SHALL be settable globally in settings or per-table in tables.yaml
5. THE default value SHALL be false (backward compatible)

### Requirement 7: Identifier Generation

**User Story:** As a developer, I want identifiers to be cryptographically random, so that they cannot be predicted or reverse-engineered.

#### Acceptance Criteria

1. THE Obfuscation_Module SHALL use cryptographically secure random number generation
2. THE Obfuscation_Module SHALL generate identifiers of sufficient length (16 characters minimum)
3. THE Obfuscation_Module SHALL verify uniqueness before using an identifier
4. THE Obfuscation_Module SHALL use lowercase hexadecimal characters only

### Requirement 8: Error Handling

**User Story:** As a user, I want clear error messages if obfuscation fails, so that I can troubleshoot issues.

#### Acceptance Criteria

1. IF identifier generation fails, THEN THE Export_System SHALL log the error and retry
2. IF master index encryption fails, THEN THE Export_System SHALL abort the export
3. IF master index cannot be decrypted during import, THEN THE Import_System SHALL provide a clear error message
4. THE Export_System SHALL validate all identifiers before use

### Requirement 9: Audit Trail

**User Story:** As a compliance officer, I want obfuscation details recorded in logs, so that I can audit data transfers.

#### Acceptance Criteria

1. THE Export_System SHALL log when obfuscation is enabled
2. THE Export_System SHALL log the folder identifier for each table (in secure logs only)
3. THE Export_System SHALL include obfuscation status in the manifest
4. THE Import_System SHALL log when processing obfuscated exports

### Requirement 10: Master Index Format

**User Story:** As a developer, I want a well-defined master index format, so that the system can reliably map identifiers to tables.

#### Acceptance Criteria

1. THE Master_Index SHALL use JSON format with the following structure:
   ```json
   {
     "version": "1.0",
     "created_at": "ISO8601 timestamp",
     "obfuscation_enabled": true,
     "tables": [
       {
         "table_name": "ACTUAL_TABLE_NAME",
         "folder_id": "a7f3d9e2c4b8f1a9",
         "export_timestamp": "ISO8601 timestamp"
       }
     ]
   }
   ```
2. THE Master_Index SHALL include a version field for future compatibility
3. THE Master_Index SHALL be encrypted using AES-256-GCM (same as data files)

## Non-Functional Requirements

### Security
- Identifiers must be cryptographically random (not predictable)
- Master index must be encrypted with same security level as data files
- No sensitive information in unencrypted file names or paths

### Performance
- Identifier generation should not significantly impact export time
- Master index should be small (< 1MB even for 1000 tables)

### Usability
- Obfuscation should be transparent to users (automatic)
- Import should work seamlessly with obfuscated exports
- Clear error messages if password is wrong

### Compatibility
- Must work with existing encryption and compression
- Must be backward compatible (can be disabled)
- Must work with filtered exports
