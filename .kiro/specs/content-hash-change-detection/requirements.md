# Requirements Document

## Introduction

This feature implements content-based change detection for the data export process to prevent unnecessary file writes and Git commits when the actual data content hasn't changed. When running `export_data.py` multiple times with the same data, the system should detect that the encrypted chunk files would be identical and skip writing them, keeping the Git repository clean and avoiding unnecessary disk I/O.

## Glossary

- **Export_System**: The data export pipeline that extracts data from Snowflake and creates encrypted chunk files
- **Content_Hash**: A cryptographic hash (SHA-256) computed from the unencrypted chunk data before encryption
- **Chunk_File**: An encrypted Parquet file containing a portion of table data
- **Manifest_File**: A JSON file (encrypted or plain) containing metadata about the export including chunk information
- **Git_Repository**: The version control system tracking the exported chunk files

## Requirements

### Requirement 1: Content Hash Computation

**User Story:** As a data engineer, I want the system to compute content hashes of chunk data before encryption, so that I can detect when the underlying data hasn't changed.

#### Acceptance Criteria

1. WHEN the Export_System processes a data chunk, THE Export_System SHALL compute a SHA-256 hash of the unencrypted Parquet file content
2. WHEN computing the content hash, THE Export_System SHALL use the raw bytes of the Parquet file before encryption
3. THE Export_System SHALL compute content hashes consistently such that identical data produces identical hashes across multiple runs
4. WHEN a chunk is processed, THE Export_System SHALL store the content hash in memory for comparison

### Requirement 2: Existing File Detection and Comparison

**User Story:** As a data engineer, I want the system to check if chunk files already exist and compare their content, so that unchanged files are not rewritten.

#### Acceptance Criteria

1. WHEN processing a chunk, THE Export_System SHALL check if the target encrypted file already exists on disk
2. IF an encrypted file exists, THEN THE Export_System SHALL decrypt it temporarily to compute its content hash
3. WHEN comparing content, THE Export_System SHALL compare the new chunk's content hash with the existing file's content hash
4. IF the content hashes match, THEN THE Export_System SHALL skip writing the file and reuse the existing file
5. IF the content hashes differ, THEN THE Export_System SHALL write the new encrypted file, replacing the old one

### Requirement 3: Conditional File Writing

**User Story:** As a data engineer, I want files to be written only when content has changed, so that Git doesn't track unnecessary changes.

#### Acceptance Criteria

1. WHEN a chunk's content hash matches the existing file's hash, THE Export_System SHALL not write the encrypted file
2. WHEN a chunk's content hash differs from the existing file's hash, THE Export_System SHALL write the new encrypted file
3. WHEN no existing file is found, THE Export_System SHALL write the encrypted file
4. WHEN skipping a file write, THE Export_System SHALL log the decision with the file name and reason
5. WHEN writing a file, THE Export_System SHALL log that the file was updated due to content changes

### Requirement 4: Export Statistics and Reporting

**User Story:** As a data engineer, I want to see statistics about which files were skipped or updated, so that I can verify the change detection is working correctly.

#### Acceptance Criteria

1. WHEN an export completes, THE Export_System SHALL report the total number of chunks processed
2. WHEN an export completes, THE Export_System SHALL report the number of chunks that were skipped (unchanged)
3. WHEN an export completes, THE Export_System SHALL report the number of chunks that were written (new or changed)
4. WHEN an export completes, THE Export_System SHALL display the percentage of chunks that were unchanged
5. THE Export_System SHALL include these statistics in the console output at the end of each table export

### Requirement 5: Manifest Consistency

**User Story:** As a data engineer, I want the manifest file to accurately reflect the current state of chunk files, so that imports work correctly even when some chunks were skipped.

#### Acceptance Criteria

1. WHEN generating the manifest, THE Export_System SHALL include metadata for all chunks regardless of whether they were skipped or written
2. WHEN a chunk is skipped, THE Export_System SHALL read the existing file's metadata (size, checksum) for the manifest
3. WHEN a chunk is written, THE Export_System SHALL use the new file's metadata for the manifest
4. THE Export_System SHALL ensure the manifest accurately represents the final state of all chunk files
5. WHEN the manifest itself hasn't changed in content, THE Export_System SHALL skip writing it

### Requirement 6: Performance Optimization

**User Story:** As a data engineer, I want the change detection to be efficient, so that it doesn't significantly slow down the export process.

#### Acceptance Criteria

1. WHEN checking for existing files, THE Export_System SHALL only decrypt the minimum necessary data to compute the content hash
2. THE Export_System SHALL reuse the encryption password across all file operations without re-prompting
3. WHEN processing multiple chunks, THE Export_System SHALL perform hash comparisons in memory without creating temporary files
4. THE Export_System SHALL complete the hash comparison for an existing file within 2 seconds for files up to 100MB

### Requirement 7: Error Handling for Corrupted Files

**User Story:** As a data engineer, I want the system to handle corrupted existing files gracefully, so that exports can complete even if old files are damaged.

#### Acceptance Criteria

1. IF decrypting an existing file fails, THEN THE Export_System SHALL log a warning and treat the file as changed
2. IF computing a hash for an existing file fails, THEN THE Export_System SHALL log a warning and write the new file
3. WHEN encountering a corrupted file, THE Export_System SHALL not halt the entire export process
4. WHEN replacing a corrupted file, THE Export_System SHALL log that the file was replaced due to corruption

### Requirement 8: Backward Compatibility

**User Story:** As a data engineer, I want the change detection feature to work with existing export folders, so that I don't need to re-export all data.

#### Acceptance Criteria

1. WHEN processing an export folder with existing files, THE Export_System SHALL correctly detect and compare them
2. THE Export_System SHALL work with both obfuscated and non-obfuscated file naming schemes
3. THE Export_System SHALL work with both encrypted manifest files and plain JSON manifest files
4. WHEN the --clean flag is used, THE Export_System SHALL delete all existing files and perform a full export as before
