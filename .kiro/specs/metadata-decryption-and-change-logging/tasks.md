# Implementation Plan: Metadata Decryption and Change Logging

## Overview

This implementation adds persistent change logging, decryption utilities, and obfuscation-aware change tracking to the metadata management system. The implementation is organized into logical phases that build upon each other.

## Tasks

- [x] 1. Enhance ChangeLogger with persistent file logging
  - Update `pipeline/utils/change_logger.py` to write to persistent log files
  - Implement log file creation in `metadata/changes/` directory
  - Implement change entry formatting with timestamps and separators
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

- [ ]* 1.1 Write unit tests for ChangeLogger file operations
  - Test log file creation
  - Test change entry formatting
  - Test append behavior
  - Test directory creation
  - _Requirements: 1.1, 1.3, 1.7_

- [ ]* 1.2 Write property test for change log persistence
  - **Property 1: Change Log Persistence**
  - **Validates: Requirements 1.1, 1.3**

- [ ]* 1.3 Write property test for append behavior
  - **Property 2: Change Log Append Behavior**
  - **Validates: Requirements 1.3**

- [x] 2. Add change history retrieval methods to ChangeLogger
  - Implement `get_change_history()` method
  - Implement `get_changes_by_date_range()` method
  - Implement change log parsing logic
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [ ]* 2.1 Write unit tests for change history retrieval
  - Test retrieving all changes
  - Test retrieving with limit
  - Test date range filtering
  - Test handling missing log files
  - _Requirements: 8.1, 8.2, 8.4, 8.5_

- [ ]* 2.2 Write property test for change history retrieval
  - **Property 11: Change History Retrieval**
  - **Validates: Requirements 8.2**

- [x] 3. Create MetadataDecryptor utility class
  - Create `pipeline/utils/metadata_decryptor.py`
  - Implement `__init__()` with directory paths
  - Implement `decrypt_master_index()` method
  - Implement `decrypt_table()` method for single table
  - Implement `decrypt_all_tables()` method
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.7_

- [ ]* 3.1 Write unit tests for MetadataDecryptor
  - Test master index decryption
  - Test single table decryption
  - Test all tables decryption
  - Test file naming conventions
  - _Requirements: 3.1, 3.3, 3.4, 3.7_

- [ ]* 3.2 Write property test for decryption round trip
  - **Property 3: Decryption Round Trip**
  - **Validates: Requirements 3.1, 3.4**

- [ ]* 3.3 Write property test for decrypted file naming
  - **Property 4: Decrypted File Naming**
  - **Validates: Requirements 3.3**

- [x] 4. Add decrypted folder management to MetadataDecryptor
  - Implement `clean_decrypted_files()` method
  - Implement `ensure_gitignore()` method
  - Implement directory structure creation
  - Add `.gitignore` entry for `metadata/decrypted/`
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ]* 4.1 Write unit tests for folder management
  - Test cleanup functionality
  - Test gitignore management
  - Test directory creation
  - _Requirements: 4.1, 4.2, 4.3_

- [ ]* 4.2 Write property test for git ignore protection
  - **Property 5: Git Ignore Protection**
  - **Validates: Requirements 4.2**

- [ ]* 4.3 Write property test for directory creation idempotence
  - **Property 12: Directory Creation Idempotence**
  - **Validates: Requirements 1.7, 4.1**

- [ ] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Add error handling to MetadataDecryptor
  - Implement error handling for incorrect passwords
  - Implement error handling for missing master index
  - Implement error handling for table not found
  - Implement `list_available_tables()` method
  - _Requirements: 3.6, 10.1, 10.2, 10.3_

- [ ]* 6.1 Write unit tests for error handling
  - Test incorrect password handling
  - Test missing master index handling
  - Test table not found handling
  - _Requirements: 3.6, 10.1, 10.2, 10.3_

- [ ]* 6.2 Write property test for decryption error handling
  - **Property 10: Decryption Error Handling**
  - **Validates: Requirements 3.6, 10.1**

- [x] 7. Enhance MetadataExtractor for obfuscated change tracking
  - Add `check_metadata_changed_obfuscated()` method
  - Add `archive_old_metadata_obfuscated()` method
  - Update `save_metadata_to_file()` to support obfuscated change tracking
  - Update `save_metadata_to_file()` to pass archived file paths to ChangeLogger
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [ ]* 7.1 Write unit tests for obfuscated change tracking
  - Test change detection with obfuscated files
  - Test archiving obfuscated files
  - Test deterministic file ID generation
  - Test master index updates
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [ ]* 7.2 Write property test for change detection with obfuscation
  - **Property 6: Change Detection with Obfuscation**
  - **Validates: Requirements 5.1, 5.5**

- [ ]* 7.3 Write property test for archive file naming
  - **Property 7: Archive File Naming**
  - **Validates: Requirements 5.4**

- [ ]* 7.4 Write property test for master index consistency
  - **Property 8: Master Index Consistency**
  - **Validates: Requirements 5.3**

- [x] 8. Update ChangeLogger integration in MetadataExtractor
  - Update calls to `change_logger.log_change()` to include archived file paths
  - Update calls to `change_logger.log_initial_extraction()` to include created file paths
  - Ensure change logging works in both obfuscated and non-obfuscated modes
  - _Requirements: 2.7, 5.5, 9.1, 9.2, 9.3_

- [ ]* 8.1 Write integration tests for change logging
  - Test change logging in non-obfuscated mode
  - Test change logging in obfuscated mode
  - Test archived file paths in log entries
  - _Requirements: 2.7, 5.5, 9.2, 9.3_

- [ ]* 8.2 Write property test for change log format consistency
  - **Property 9: Change Log Format Consistency**
  - **Validates: Requirements 1.4**

- [ ] 9. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Create decrypt_metadata.py script
  - Create `scripts/decrypt_metadata.py`
  - Implement command-line argument parsing
  - Implement `--all` option to decrypt all tables
  - Implement `--table` option to decrypt specific table
  - Implement `--password` option with secure prompt fallback
  - Implement `--list` option to list available tables
  - Implement `--clean` option to delete decrypted files
  - Implement `--show-changes` option to display change history
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

- [ ]* 10.1 Write integration tests for decrypt_metadata script
  - Test decrypting all tables
  - Test decrypting specific table
  - Test password prompt
  - Test listing tables
  - Test cleanup
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 11. Create view_change_history.py script
  - Create `scripts/view_change_history.py`
  - Implement command-line argument parsing
  - Implement `--table` option to view specific table history
  - Implement `--limit` option to limit number of entries
  - Implement `--from` and `--to` options for date range
  - Implement `--summary` option to view all tables with changes
  - _Requirements: 8.1, 8.2, 8.3, 8.5, 8.6_

- [ ]* 11.1 Write integration tests for view_change_history script
  - Test viewing all changes
  - Test viewing with limit
  - Test date range filtering
  - Test summary view
  - _Requirements: 8.1, 8.2, 8.5_

- [x] 12. Add README files to metadata directories
  - Create `metadata/README.md` explaining directory structure
  - Create `metadata/changes/README.md` explaining change logs
  - Create `metadata/decrypted/README.md` explaining temporary nature
  - _Requirements: 7.6_

- [ ] 13. Update existing documentation
  - Update `docs/metadata-change-tracking.md` with persistent logging info
  - Update `docs/command-reference.md` with new script commands
  - Update `README.md` with decryption utility usage
  - Create `docs/metadata-decryption-guide.md` with comprehensive guide
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [ ] 14. Final integration testing
  - Test complete workflow: extract → detect changes → log → decrypt → view
  - Test with obfuscation enabled
  - Test with obfuscation disabled
  - Test backward compatibility with existing workflows
  - Test error scenarios (wrong password, missing files, etc.)
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

- [ ] 15. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- Integration tests validate end-to-end workflows
- The implementation maintains backward compatibility throughout
