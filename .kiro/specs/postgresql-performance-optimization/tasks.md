# Implementation Plan: PostgreSQL Performance Optimization

## Overview

This implementation plan breaks down the index configuration feature into discrete coding tasks. Each task builds on previous work, starting with configuration schema updates, then validation, DDL generation, and finally comprehensive testing. The plan ensures backward compatibility while adding the new index functionality.

## Tasks

- [ ] 1. Update configuration data models and YAML parsing
  - Add `indexes` field to PostgresConfig dataclass with default empty list
  - Update YAML parsing to handle optional indexes field
  - Ensure backward compatibility for configs without indexes field
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [ ]* 1.1 Write property test for configuration parsing
  - **Property 1: Configuration Parsing Completeness**
  - **Validates: Requirements 1.1, 1.2**

- [ ]* 1.2 Write property test for backward compatibility
  - **Property 2: Backward Compatibility**
  - **Validates: Requirements 1.3, 4.1, 4.2**

- [ ] 2. Implement configuration validator
  - [ ] 2.1 Create `pipeline/utils/config_validator.py` module
    - Implement `validate_index_configuration()` function
    - Check that all index columns exist in table metadata
    - Return ValidationResult with success status and error messages
    - _Requirements: 3.1, 3.2_

  - [ ] 2.2 Add validation error classes
    - Create `ConfigurationError` and `IndexValidationError` exceptions
    - Implement descriptive error messages with available columns list
    - _Requirements: 3.2_

  - [ ]* 2.3 Write property test for validation rejection
    - **Property 5: Validation Rejects Invalid Columns**
    - **Validates: Requirements 3.1, 3.2**

  - [ ]* 2.4 Write property test for validation acceptance
    - **Property 6: Validation Accepts Valid Columns**
    - **Validates: Requirements 3.1, 3.3**

  - [ ]* 2.5 Write property test for empty index list handling
    - **Property 7: Empty Index List Handling**
    - **Validates: Requirements 2.4, 3.4**

- [ ] 3. Checkpoint - Ensure configuration and validation tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Implement DDL generation for indexes
  - [ ] 4.1 Create `generate_index_ddl()` function
    - Generate single CREATE INDEX statement with format: `CREATE INDEX idx_{table}_{column} ON {schema}.{table} ({column});`
    - Use B-tree as default index type
    - _Requirements: 2.2, 2.5_

  - [ ] 4.2 Update `generate_ddl_with_indexes()` function
    - Generate CREATE TABLE statement (existing logic)
    - Append CREATE INDEX statements for each column in indexes list
    - Ensure indexes appear after CREATE TABLE statement
    - Handle empty index list (no index statements)
    - _Requirements: 2.1, 2.3, 2.4_

  - [ ]* 4.3 Write property test for index DDL format
    - **Property 3: Index DDL Format Correctness**
    - **Validates: Requirements 2.2**

  - [ ]* 4.4 Write property test for index count consistency
    - **Property 4: Index Count Consistency**
    - **Validates: Requirements 2.1, 2.3**

  - [ ]* 4.5 Write property test for DDL ordering
    - **Property 8: DDL Ordering**
    - **Validates: Requirements 2.3**

- [ ] 5. Integrate validation and DDL generation into pipeline
  - [ ] 5.1 Update metadata extraction workflow
    - Call `validate_index_configuration()` after loading table metadata
    - Pass validated index columns to DDL generator
    - Handle validation errors with descriptive logging
    - _Requirements: 3.1, 3.2, 3.3_

  - [ ] 5.2 Update PostgreSQL loader to use new DDL generation
    - Modify `create_table_from_metadata()` to pass index configuration
    - Update DDL loading to handle index statements
    - Ensure indexes are created after table creation
    - _Requirements: 2.1, 2.3_

  - [ ]* 5.3 Write integration tests
    - Test end-to-end: Load config → Validate → Generate DDL → Verify format
    - Test with sample table metadata
    - Test error handling for invalid configurations
    - _Requirements: All_

- [ ] 6. Add logging and error handling
  - Add INFO logs for successful validation with column list
  - Add WARNING logs for duplicate columns in index list
  - Add ERROR logs for validation failures
  - Add DEBUG logs for DDL generation steps
  - _Requirements: 3.3_

- [ ] 7. Update documentation
  - Update `config/tables.yaml` with example index configuration
  - Add comments explaining the indexes field
  - Document index naming convention
  - _Requirements: 1.4_

- [ ] 8. Final checkpoint - Ensure all tests pass
  - Run full test suite (unit tests and property tests)
  - Verify backward compatibility with existing configurations
  - Test with real Snowflake metadata if available
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties (minimum 100 iterations each)
- Unit tests validate specific examples and edge cases
- Integration tests verify end-to-end functionality
