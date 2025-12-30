# Requirements: Metadata Obfuscation and Password Management

## Introduction

This specification defines requirements for adding obfuscation to metadata files (JSON and DDL), making obfuscation the default behavior, storing encryption passwords in .env, and simplifying the change tracking system.

## Glossary

- **Metadata Files**: JSON schema files and SQL DDL files containing table structure information
- **Obfuscation**: Process of randomizing file and folder names to hide data structure
- **Encryption Password**: Password used to encrypt/decrypt data and metadata files
- **Change Tracking**: System for detecting and logging metadata schema changes
- **Versioned Files**: Archived metadata files with timestamp suffixes

## Requirements

### Requirement 1: Metadata File Obfuscation

**User Story:** As a security-conscious user, I want metadata files (JSON and DDL) to be obfuscated with random names, so that casual observers cannot identify table structures from file listings.

#### Acceptance Criteria

1. WHEN metadata is extracted with obfuscation enabled, THE System SHALL generate random file IDs for metadata JSON files
2. WHEN metadata is extracted with obfuscation enabled, THE System SHALL generate random file IDs for DDL SQL files
3. WHEN metadata is extracted with obfuscation enabled, THE System SHALL encrypt metadata files using the encryption password
4. WHEN metadata is extracted with obfuscation enabled, THE System SHALL create an encrypted master index mapping table names to file IDs
5. WHEN metadata is extracted with obfuscation enabled, THE System SHALL store the master index as `index.enc` in the metadata directory

### Requirement 2: Default Obfuscation Behavior

**User Story:** As a user, I want obfuscation to be enabled by default for both data and metadata, so that I don't have to remember to enable it each time.

#### Acceptance Criteria

1. WHEN no obfuscation flag is specified, THE System SHALL enable obfuscation by default
2. WHEN the `--no-obfuscate` flag is provided, THE System SHALL disable obfuscation
3. WHEN obfuscation is enabled by default, THE System SHALL apply it to both data export and metadata extraction
4. THE System SHALL read the default obfuscation setting from the `OBFUSCATE_NAMES` environment variable
5. WHEN `OBFUSCATE_NAMES=true` in .env, THE System SHALL enable obfuscation by default
6. WHEN `OBFUSCATE_NAMES=false` in .env, THE System SHALL disable obfuscation by default

### Requirement 3: Password Storage in Environment

**User Story:** As a user, I want to store my encryption password in the .env file, so that I don't have to manually enter it every time I run export or import operations.

#### Acceptance Criteria

1. THE System SHALL read the encryption password from the `ENCRYPTION_PASSWORD` environment variable
2. WHEN `ENCRYPTION_PASSWORD` is set in .env, THE System SHALL use it for encryption/decryption operations
3. WHEN `ENCRYPTION_PASSWORD` is not set in .env, THE System SHALL prompt the user for a password
4. THE System SHALL ensure .env is excluded from Git tracking via .gitignore
5. THE System SHALL provide clear documentation on setting the encryption password in .env
6. THE System SHALL use the same password for both data and metadata encryption

### Requirement 4: Simplified Change Tracking

**User Story:** As a user, I want change tracking to be simplified by removing the separate changes folder and keeping versioned files in the schemas/ddl folders, so that all related files are in one place.

#### Acceptance Criteria

1. THE System SHALL NOT create a `metadata/changes/` directory
2. WHEN metadata changes are detected, THE System SHALL archive old files with timestamp suffix in `metadata/schemas/`
3. WHEN metadata changes are detected, THE System SHALL archive old DDL files with timestamp suffix in `metadata/ddl/`
4. THE System SHALL use the naming format `{table}_{YYYYMMDD}_metadata.json` for archived metadata
5. THE System SHALL use the naming format `{table}_{YYYYMMDD}_create.sql` for archived DDL
6. THE System SHALL log change information to the console instead of separate log files
7. THE System SHALL maintain current files as `{table}_metadata.json` and `{table}_create.sql`

### Requirement 5: Script Argument Updates

**User Story:** As a user, I want consistent command-line arguments across all scripts, so that I can easily control obfuscation behavior.

#### Acceptance Criteria

1. THE `export_data.py` script SHALL accept `--no-obfuscate` flag to disable obfuscation
2. THE `extract_metadata.py` script SHALL accept `--no-obfuscate` flag to disable obfuscation
3. THE `import_data.py` script SHALL automatically detect obfuscated exports
4. WHEN `--no-obfuscate` is not specified, THE System SHALL use the default from `OBFUSCATE_NAMES` environment variable
5. THE System SHALL provide clear help text for obfuscation flags

### Requirement 6: Environment Configuration

**User Story:** As a user, I want clear documentation and examples for configuring encryption and obfuscation in my .env file, so that I can set it up correctly.

#### Acceptance Criteria

1. THE `env.example` file SHALL include `ENCRYPTION_PASSWORD` with clear documentation
2. THE `env.example` file SHALL include `OBFUSCATE_NAMES` with default value `true`
3. THE System SHALL provide warnings if `ENCRYPTION_PASSWORD` is not set and no password is provided
4. THE System SHALL validate that the encryption password meets minimum security requirements
5. THE documentation SHALL explain the security implications of storing passwords in .env

### Requirement 7: Backward Compatibility

**User Story:** As a user with existing exports, I want the system to continue working with my non-obfuscated exports, so that I don't have to re-export everything.

#### Acceptance Criteria

1. WHEN importing data, THE System SHALL detect whether the export is obfuscated or not
2. WHEN importing non-obfuscated data, THE System SHALL process it using the original logic
3. WHEN importing obfuscated data, THE System SHALL use the master index to locate files
4. THE System SHALL support both `.export_index.enc` (legacy) and `index.enc` (new) master index files
5. THE System SHALL provide clear error messages if obfuscated data cannot be decrypted

## Non-Functional Requirements

### Security

1. Encryption passwords SHALL NOT be logged or displayed in console output
2. The .env file SHALL be excluded from Git tracking
3. Encrypted metadata files SHALL use the same AES-256-GCM encryption as data files
4. The system SHALL use the same key derivation parameters for metadata and data encryption

### Usability

1. Default behavior SHALL favor security (obfuscation enabled)
2. Users SHALL be able to opt-out of obfuscation with a single flag
3. Error messages SHALL be clear and actionable
4. Documentation SHALL provide step-by-step setup instructions

### Performance

1. Metadata obfuscation SHALL NOT significantly impact extraction time
2. Master index creation SHALL be efficient even with many tables
3. File lookups using the master index SHALL be fast

## Success Criteria

1. Users can set `ENCRYPTION_PASSWORD` in .env and never be prompted for passwords
2. Obfuscation is enabled by default for both data and metadata
3. Users can disable obfuscation with `--no-obfuscate` flag
4. Versioned metadata files are stored in schemas/ddl folders with timestamps
5. No separate changes folder is created
6. All existing functionality continues to work with backward compatibility
