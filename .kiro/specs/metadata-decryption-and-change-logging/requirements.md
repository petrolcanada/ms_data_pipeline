# Requirements Document

## Introduction

This feature enhances the metadata management system by adding persistent change logging, decryption utilities for human-readable viewing, and proper support for change tracking with obfuscated/encrypted metadata files.

## Glossary

- **Metadata_System**: The system that extracts, stores, and manages table schema information from Snowflake
- **Change_Logger**: Component that records metadata changes to persistent log files
- **Decryption_Utility**: Tool that decrypts encrypted metadata files for human viewing
- **Obfuscated_File**: Encrypted file with randomized filename (e.g., `4923cba5118f2c90.enc`)
- **Decrypted_Folder**: Local directory containing human-readable versions of encrypted files (not tracked by Git)
- **Master_Index**: Encrypted mapping file that links table names to obfuscated file IDs
- **Change_Log_File**: Persistent text file recording all metadata changes for a table

## Requirements

### Requirement 1: Persistent Change Logging

**User Story:** As a data engineer, I want all metadata changes to be logged to persistent files, so that I can review the complete change history even after the console output is gone.

#### Acceptance Criteria

1. WHEN a metadata change is detected, THE Change_Logger SHALL write the change details to a persistent log file
2. THE Change_Logger SHALL create log files in the format `metadata/changes/{table_name}_changes.log`
3. WHEN writing to a change log file, THE Change_Logger SHALL append new entries without overwriting existing content
4. THE Change_Logger SHALL include timestamps in ISO 8601 format for each log entry
5. THE Change_Logger SHALL include a separator line between log entries for readability
6. WHEN the initial metadata extraction occurs, THE Change_Logger SHALL log the initial extraction event
7. THE Change_Logger SHALL create the `metadata/changes/` directory if it does not exist

### Requirement 2: Change Log Content Format

**User Story:** As a data engineer, I want change logs to be human-readable and comprehensive, so that I can quickly understand what changed without parsing JSON files.

#### Acceptance Criteria

1. THE Change_Logger SHALL include a summary line describing the total number of changes
2. WHEN a column is added, THE Change_Logger SHALL log the column name, data type, and nullable constraint
3. WHEN a column is removed, THE Change_Logger SHALL log the column name and its previous data type
4. WHEN a column type changes, THE Change_Logger SHALL log both the old type and new type
5. WHEN a nullable constraint changes, THE Change_Logger SHALL log both the old and new nullable status
6. WHEN a primary key changes, THE Change_Logger SHALL log both the old and new primary key columns
7. THE Change_Logger SHALL include the paths to archived metadata and DDL files in each log entry

### Requirement 3: Metadata Decryption Utility

**User Story:** As a data engineer, I want to decrypt encrypted metadata files on-demand, so that I can view and debug schema information without compromising security.

#### Acceptance Criteria

1. THE Decryption_Utility SHALL decrypt encrypted metadata files using a provided password
2. THE Decryption_Utility SHALL save decrypted files to a separate `metadata/decrypted/` directory
3. THE Decryption_Utility SHALL preserve the original table names in decrypted filenames
4. WHEN decrypting metadata, THE Decryption_Utility SHALL decrypt both JSON metadata and SQL DDL files
5. THE Decryption_Utility SHALL decrypt the master index file to enable table name lookups
6. WHEN a decryption password is incorrect, THE Decryption_Utility SHALL return a clear error message
7. THE Decryption_Utility SHALL support decrypting all tables or a specific table

### Requirement 4: Decrypted Folder Management

**User Story:** As a data engineer, I want decrypted files to be automatically excluded from Git, so that sensitive information is never accidentally committed.

#### Acceptance Criteria

1. THE Metadata_System SHALL create a `metadata/decrypted/` directory structure with `schemas/` and `ddl/` subdirectories
2. THE Metadata_System SHALL add `metadata/decrypted/` to `.gitignore` if not already present
3. WHEN decrypted files exist, THE Decryption_Utility SHALL provide an option to clean up (delete) all decrypted files
4. THE Decryption_Utility SHALL preserve the encrypted files when cleaning up decrypted files
5. THE Metadata_System SHALL organize decrypted files with the same structure as encrypted files

### Requirement 5: Change Tracking with Obfuscated Files

**User Story:** As a data engineer, I want change tracking to work seamlessly with obfuscated files, so that I can use encryption without losing change detection capabilities.

#### Acceptance Criteria

1. WHEN obfuscation is enabled, THE Metadata_System SHALL decrypt the previous metadata file before comparing
2. WHEN changes are detected in obfuscated mode, THE Metadata_System SHALL archive the old encrypted file with a timestamp
3. THE Metadata_System SHALL maintain deterministic file IDs for metadata and DDL files across runs
4. WHEN archiving obfuscated files, THE Metadata_System SHALL use the format `{file_id}_{YYYYMMDD}.enc`
5. THE Change_Logger SHALL log changes using the original table name, not the obfuscated file ID
6. THE Metadata_System SHALL update the master index when metadata files are archived

### Requirement 6: Decryption Script Interface

**User Story:** As a data engineer, I want a command-line script to decrypt metadata files, so that I can easily view encrypted schemas during development and debugging.

#### Acceptance Criteria

1. THE Decryption_Utility SHALL provide a command-line script `scripts/decrypt_metadata.py`
2. WHEN invoked with `--all`, THE Decryption_Utility SHALL decrypt all tables in the master index
3. WHEN invoked with `--table <name>`, THE Decryption_Utility SHALL decrypt only the specified table
4. THE Decryption_Utility SHALL accept a `--password` parameter for the decryption password
5. WHEN `--password` is not provided, THE Decryption_Utility SHALL prompt for password input securely
6. THE Decryption_Utility SHALL provide a `--clean` option to delete all decrypted files
7. THE Decryption_Utility SHALL display a summary of decrypted files and their locations

### Requirement 7: Folder Structure Reorganization

**User Story:** As a data engineer, I want a clear separation between encrypted and decrypted files, so that I can easily understand which files are secure and which are temporary.

#### Acceptance Criteria

1. THE Metadata_System SHALL maintain encrypted files in `metadata/schemas/` and `metadata/ddl/`
2. THE Metadata_System SHALL maintain decrypted files in `metadata/decrypted/schemas/` and `metadata/decrypted/ddl/`
3. THE Metadata_System SHALL maintain change logs in `metadata/changes/`
4. THE Metadata_System SHALL maintain the master index at `metadata/index.enc` (when obfuscation is enabled)
5. THE Metadata_System SHALL maintain archived encrypted files in the same directories as current files
6. THE Metadata_System SHALL include clear README files in each directory explaining their purpose

### Requirement 8: Change Log Viewing Utility

**User Story:** As a data engineer, I want to easily view change history for a table, so that I can understand how the schema has evolved over time.

#### Acceptance Criteria

1. THE Change_Logger SHALL provide a method to retrieve the complete change history for a table
2. THE Change_Logger SHALL provide a method to retrieve the N most recent changes for a table
3. THE Change_Logger SHALL format change history in a human-readable format
4. WHEN no change log exists for a table, THE Change_Logger SHALL return an appropriate message
5. THE Change_Logger SHALL support filtering changes by date range
6. THE Change_Logger SHALL provide a summary of total changes recorded for a table

### Requirement 9: Integration with Existing Workflow

**User Story:** As a data engineer, I want these new features to integrate seamlessly with my existing metadata extraction workflow, so that I don't need to change my current processes.

#### Acceptance Criteria

1. THE Metadata_System SHALL maintain backward compatibility with non-obfuscated metadata extraction
2. WHEN `--check-changes` is enabled, THE Metadata_System SHALL automatically write to persistent change logs
3. THE Metadata_System SHALL work correctly whether obfuscation is enabled or disabled
4. THE Metadata_System SHALL not require decryption for normal metadata extraction operations
5. THE Decryption_Utility SHALL be a separate optional tool that does not affect extraction workflows

### Requirement 10: Error Handling and Validation

**User Story:** As a data engineer, I want clear error messages when decryption or change logging fails, so that I can quickly diagnose and fix issues.

#### Acceptance Criteria

1. WHEN a decryption password is incorrect, THE Decryption_Utility SHALL display a clear error message
2. WHEN the master index file is missing, THE Decryption_Utility SHALL display a helpful error message
3. WHEN a table is not found in the master index, THE Decryption_Utility SHALL list available tables
4. WHEN change log writing fails, THE Change_Logger SHALL log the error but not fail the extraction
5. WHEN the decrypted directory cannot be created, THE Decryption_Utility SHALL display a clear error message
6. THE Metadata_System SHALL validate that required directories exist before writing files
