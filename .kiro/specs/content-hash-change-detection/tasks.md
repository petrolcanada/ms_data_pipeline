# Implementation Plan: Content-Based Change Detection

## Overview

This implementation adds content-based change detection to the data export pipeline. The system will compute SHA-256 hashes of chunk data and compare them with existing files to avoid unnecessary writes, keeping the Git repository clean.

## Tasks

- [x] 1. Create ContentHashComparator utility class
  - Create `pipeline/utils/content_hash_comparator.py`
  - Implement `compute_file_hash()` method for SHA-256 computation
  - Implement `decrypt_and_hash()` method for existing file comparison
  - Implement `should_write_file()` method for write decision logic
  - Add comprehensive error handling for decryption failures
  - Add logging for all operations
  - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.3, 7.1, 7.2_

- [ ]* 1.1 Write property test for hash consistency
  - **Property 1: Hash Consistency**
  - **Validates: Requirements 1.3**

- [ ]* 1.2 Write property test for decryption round-trip
  - **Property 4: Decryption Round-Trip**
  - **Validates: Requirements 2.2**

- [ ]* 1.3 Write unit tests for ContentHashComparator
  - Test hash computation for identical files
  - Test hash computation for different files
  - Test decryption and hashing success path
  - Test decryption failure handling
  - Test should_write_file for new files
  - Test should_write_file for unchanged files
  - Test should_write_file for changed files
  - Test should_write_file for corrupted files
  - _Requirements: 1.1, 2.1, 2.2, 2.3, 7.1, 7.2_

- [x] 2. Add ExportStatistics data class
  - Create `ExportStatistics` dataclass in `scripts/export_data.py`
  - Add fields: total_chunks, chunks_new, chunks_changed, chunks_unchanged, manifest_written
  - Add computed properties: chunks_written, unchanged_percentage
  - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [x] 3. Modify export_table function for change detection
  - [x] 3.1 Initialize ContentHashComparator and statistics tracking
    - Import ContentHashComparator
    - Initialize comparator with encryptor instance
    - Initialize ExportStatistics counters
    - _Requirements: 1.1, 4.1_

  - [x] 3.2 Implement chunk-level change detection
    - After saving Parquet, compute content hash
    - Call should_write_file() to determine if write needed
    - Conditionally encrypt and write based on decision
    - Update statistics counters (new/changed/skipped)
    - Collect metadata from new or existing file
    - Add logging for skip/write decisions
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4, 3.5_

  - [ ]* 3.3 Write property test for identical content detection
    - **Property 2: Identical Content Detection**
    - **Validates: Requirements 2.3, 3.1**

  - [ ]* 3.4 Write property test for changed content detection
    - **Property 3: Changed Content Detection**
    - **Validates: Requirements 2.3, 3.2**

- [x] 4. Implement manifest change detection
  - After creating manifest dictionary, serialize to JSON
  - Compute hash of manifest JSON string
  - Check if manifest file exists (handle both encrypted and plain)
  - If exists, read and compute hash of existing manifest
  - Compare hashes and conditionally write manifest
  - Update manifest_written flag in statistics
  - Add logging for manifest skip/write decisions
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ]* 4.1 Write property test for manifest consistency
  - **Property 6: Manifest Consistency**
  - **Validates: Requirements 5.1, 5.4**

- [x] 5. Add statistics reporting to export output
  - Display total chunks processed
  - Display chunks skipped (unchanged)
  - Display chunks written (new + changed)
  - Display percentage unchanged
  - Display manifest write status
  - Format output clearly in console
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ]* 5.1 Write property test for statistics accuracy
  - **Property 5: Statistics Accuracy**
  - **Validates: Requirements 4.1, 4.2, 4.3**

- [x] 6. Add error handling for edge cases
  - Handle corrupted existing files gracefully
  - Ensure temporary files are cleaned up on errors
  - Add appropriate warning logs for recoverable errors
  - Ensure export continues even when individual files fail comparison
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [ ]* 6.1 Write property test for corruption resilience
  - **Property 8: Corruption Resilience**
  - **Validates: Requirements 7.1, 7.2, 7.3**

- [x] 7. Checkpoint - Ensure all tests pass
  - Run all unit tests
  - Run all property tests
  - Verify no regressions in existing functionality
  - Ask the user if questions arise

- [ ]* 8. Write integration tests for end-to-end scenarios
  - Test export twice with no changes (all files skipped)
  - Test export with data changes (changed files written)
  - Test export with partial changes (mixed skip/write)
  - Test export with corrupted existing files
  - Test statistics reporting accuracy
  - Test manifest change detection
  - Test with both obfuscated and non-obfuscated modes
  - Test with --clean flag (should delete and recreate all)
  - _Requirements: 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 4.1, 4.2, 4.3, 7.1, 7.3, 8.1, 8.2, 8.3, 8.4_

- [ ] 9. Final checkpoint - Manual testing and verification
  - Ensure all tests pass, ask the user if questions arise

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- Integration tests verify end-to-end workflows
- The implementation maintains backward compatibility with existing exports
- No changes to file formats or CLI arguments
- The --clean flag behavior remains unchanged
