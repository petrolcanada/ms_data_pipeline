# Design Document: PostgreSQL Performance Optimization

## Overview

This feature extends the existing DDL generation system to support automatic index creation for PostgreSQL tables. Users will specify index columns in the `config/tables.yaml` file, and the system will generate appropriate CREATE INDEX statements alongside CREATE TABLE statements. The design maintains backward compatibility with existing configurations while adding validation to catch configuration errors early.

## Architecture

The feature integrates into the existing metadata extraction and DDL generation pipeline:

```
config/tables.yaml → Table Config Loader → Validator → DDL Generator → PostgreSQL
```

### Key Components:
1. **Configuration Schema Extension**: Add optional `indexes` field to table configuration
2. **Configuration Validator**: Validate index columns against table metadata
3. **DDL Generator Enhancement**: Generate CREATE INDEX statements
4. **PostgreSQL Loader Integration**: Execute index creation after table creation

## Components and Interfaces

### 1. Table Configuration Schema (config/tables.yaml)

Extended YAML structure:

```yaml
tables:
  - name: "TABLE_NAME"
    snowflake:
      database: "DB"
      schema: "SCHEMA"
      table: "TABLE"
    postgres:
      schema: "schema"
      table: "table_name"
      indexes:  # NEW: Optional list of columns to index
        - "column1"
        - "column2"
```

### 2. Configuration Loader

**Location**: `pipeline/config/table_config.py` (or new module if needed)

**Interface**:
```python
def load_table_config(config_path: str) -> List[TableConfig]:
    """Load and parse table configuration from YAML"""
    pass

class TableConfig:
    name: str
    snowflake: SnowflakeConfig
    postgres: PostgresConfig
    
class PostgresConfig:
    schema: str
    table: str
    indexes: List[str] = []  # NEW: defaults to empty list
```

### 3. Configuration Validator

**Location**: New module `pipeline/utils/config_validator.py`

**Interface**:
```python
def validate_index_configuration(
    table_config: TableConfig,
    table_metadata: Dict[str, Any]
) -> ValidationResult:
    """
    Validate that all configured index columns exist in table metadata.
    
    Args:
        table_config: Configuration for the table
        table_metadata: Metadata extracted from Snowflake
        
    Returns:
        ValidationResult with success status and error messages
        
    Raises:
        ConfigurationError: If validation fails
    """
    pass

class ValidationResult:
    success: bool
    errors: List[str]
    warnings: List[str]
```

### 4. DDL Generator

**Location**: Enhancement to existing DDL generation (likely in `pipeline/extractors/metadata_extractor.py` or new module)

**Interface**:
```python
def generate_ddl_with_indexes(
    table_metadata: Dict[str, Any],
    postgres_schema: str,
    postgres_table: str,
    index_columns: List[str]
) -> str:
    """
    Generate CREATE TABLE statement followed by CREATE INDEX statements.
    
    Args:
        table_metadata: Table structure from Snowflake
        postgres_schema: Target PostgreSQL schema
        postgres_table: Target PostgreSQL table name
        index_columns: List of columns to index
        
    Returns:
        Complete DDL script with table and index creation
    """
    pass

def generate_index_ddl(
    schema: str,
    table: str,
    column: str
) -> str:
    """
    Generate a single CREATE INDEX statement.
    
    Format: CREATE INDEX idx_{table}_{column} ON {schema}.{table} ({column});
    
    Args:
        schema: PostgreSQL schema name
        table: PostgreSQL table name
        column: Column name to index
        
    Returns:
        CREATE INDEX statement
    """
    pass
```

## Data Models

### Configuration Data Model

```python
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class SnowflakeConfig:
    database: str
    schema: str
    table: str
    filter: Optional[List[str]] = None

@dataclass
class PostgresConfig:
    schema: str
    table: str
    indexes: List[str] = field(default_factory=list)

@dataclass
class TableConfig:
    name: str
    snowflake: SnowflakeConfig
    postgres: PostgresConfig
```

### Metadata Data Model

```python
@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    nullable: bool
    primary_key: bool

@dataclass
class TableMetadata:
    table_name: str
    columns: List[ColumnMetadata]
    primary_keys: List[str]
```

## Implementation Details

### DDL Generation Algorithm

```
1. Load table configuration from YAML
2. Load table metadata from Snowflake
3. Validate index columns against metadata
4. Generate CREATE TABLE statement (existing logic)
5. For each column in indexes list:
   a. Generate CREATE INDEX statement
   b. Append to DDL script
6. Return complete DDL script
```

### Index Naming Convention

Format: `idx_{table}_{column}`

Examples:
- Table: `fund_data`, Column: `fund_id` → Index: `idx_fund_data_fund_id`
- Table: `market_prices`, Column: `date` → Index: `idx_market_prices_date`

### Index Type

Default to B-tree indexes (PostgreSQL default):
```sql
CREATE INDEX idx_table_column ON schema.table (column);
```

B-tree is appropriate for:
- Equality comparisons (`=`)
- Range queries (`<`, `>`, `BETWEEN`)
- Sorting operations (`ORDER BY`)
- Pattern matching with leading characters (`LIKE 'prefix%'`)

### Error Handling

1. **Missing Column Error**:
   - When: Index column not found in table metadata
   - Action: Raise `ConfigurationError` with descriptive message
   - Message: "Index column '{column}' not found in table '{table}'. Available columns: {column_list}"

2. **Empty Index List**:
   - When: `indexes: []` specified
   - Action: Skip validation and index generation (no error)

3. **Missing Index Section**:
   - When: `indexes` field omitted from configuration
   - Action: Treat as empty list, no indexes generated (backward compatible)

4. **Duplicate Column**:
   - When: Same column listed multiple times in indexes
   - Action: Log warning, create index only once

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Configuration Parsing Completeness
*For any* valid YAML table configuration with an indexes section, parsing the configuration should preserve all specified index columns in the resulting TableConfig object.

**Validates: Requirements 1.1, 1.2**

### Property 2: Backward Compatibility
*For any* table configuration without an indexes section, the configuration loader should treat it as having an empty index list and generate DDL without index statements.

**Validates: Requirements 1.3, 4.1, 4.2**

### Property 3: Index DDL Format Correctness
*For any* valid table name, schema name, and column name, the generated CREATE INDEX statement should match the format: `CREATE INDEX idx_{table}_{column} ON {schema}.{table} ({column});`

**Validates: Requirements 2.2**

### Property 4: Index Count Consistency
*For any* table configuration with N index columns, the generated DDL should contain exactly N CREATE INDEX statements (one per column).

**Validates: Requirements 2.1, 2.3**

### Property 5: Validation Rejects Invalid Columns
*For any* index column name that does not exist in the table metadata, validation should fail with a descriptive error message indicating the invalid column.

**Validates: Requirements 3.1, 3.2**

### Property 6: Validation Accepts Valid Columns
*For any* index column name that exists in the table metadata, validation should succeed and log a confirmation message.

**Validates: Requirements 3.1, 3.3**

### Property 7: Empty Index List Handling
*For any* table configuration with an empty indexes list, validation should succeed without errors and DDL generation should produce no CREATE INDEX statements.

**Validates: Requirements 2.4, 3.4**

### Property 8: DDL Ordering
*For any* generated DDL script with indexes, all CREATE INDEX statements should appear after the CREATE TABLE statement.

**Validates: Requirements 2.3**

## Testing Strategy

### Unit Tests

Unit tests will verify specific examples and edge cases:

1. **Configuration Parsing Tests**:
   - Parse configuration with indexes
   - Parse configuration without indexes
   - Parse configuration with empty indexes list
   - Parse invalid YAML (error handling)

2. **Validation Tests**:
   - Validate with all valid columns
   - Validate with one invalid column
   - Validate with multiple invalid columns
   - Validate with empty index list
   - Validate with duplicate columns

3. **DDL Generation Tests**:
   - Generate DDL with no indexes
   - Generate DDL with one index
   - Generate DDL with multiple indexes
   - Verify index naming convention
   - Verify DDL ordering (CREATE TABLE before CREATE INDEX)

4. **Integration Tests**:
   - End-to-end: Load config → Validate → Generate DDL → Verify format
   - Test with real table metadata from Snowflake
   - Test PostgreSQL execution of generated DDL

### Property-Based Tests

Property-based tests will verify universal properties across many generated inputs using **Hypothesis** (Python's property-based testing library). Each test will run a minimum of 100 iterations.

1. **Property Test: Configuration Round-Trip**
   - Generate random table configurations with various index lists
   - Parse to TableConfig object
   - Verify all index columns preserved
   - **Feature: postgresql-performance-optimization, Property 1: Configuration Parsing Completeness**

2. **Property Test: Backward Compatibility**
   - Generate random table configurations without indexes field
   - Verify parsed config has empty index list
   - Verify generated DDL contains no CREATE INDEX statements
   - **Feature: postgresql-performance-optimization, Property 2: Backward Compatibility**

3. **Property Test: Index DDL Format**
   - Generate random valid schema, table, and column names
   - Generate CREATE INDEX statement
   - Verify format matches regex: `CREATE INDEX idx_\w+_\w+ ON \w+\.\w+ \(\w+\);`
   - **Feature: postgresql-performance-optimization, Property 3: Index DDL Format Correctness**

4. **Property Test: Index Count**
   - Generate random table config with N index columns (0 ≤ N ≤ 20)
   - Generate DDL
   - Count CREATE INDEX statements in output
   - Verify count equals N
   - **Feature: postgresql-performance-optimization, Property 4: Index Count Consistency**

5. **Property Test: Validation Rejection**
   - Generate random table metadata with known columns
   - Generate random index column names NOT in metadata
   - Verify validation fails with descriptive error
   - **Feature: postgresql-performance-optimization, Property 5: Validation Rejects Invalid Columns**

6. **Property Test: Validation Acceptance**
   - Generate random table metadata with known columns
   - Generate random index column names from metadata columns
   - Verify validation succeeds
   - **Feature: postgresql-performance-optimization, Property 6: Validation Accepts Valid Columns**

7. **Property Test: Empty Index Handling**
   - Generate random table configs with empty index lists
   - Verify validation succeeds
   - Verify DDL contains zero CREATE INDEX statements
   - **Feature: postgresql-performance-optimization, Property 7: Empty Index List Handling**

8. **Property Test: DDL Ordering**
   - Generate random table configs with indexes
   - Generate DDL
   - Verify CREATE TABLE appears before all CREATE INDEX statements
   - **Feature: postgresql-performance-optimization, Property 8: DDL Ordering**

### Test Configuration

- **Framework**: pytest with Hypothesis plugin
- **Iterations**: Minimum 100 per property test
- **Test Data**: Use Hypothesis strategies for generating:
  - Valid SQL identifiers (table/column names)
  - Table metadata structures
  - YAML configuration structures
  - Lists of varying lengths

### Testing Approach Balance

- **Unit tests** focus on specific examples, edge cases (empty lists, missing fields), and error conditions
- **Property tests** verify universal correctness across many randomly generated inputs
- Both approaches are complementary: unit tests catch concrete bugs, property tests verify general correctness

## Error Handling

### Configuration Errors

```python
class ConfigurationError(Exception):
    """Raised when table configuration is invalid"""
    pass

class IndexValidationError(ConfigurationError):
    """Raised when index configuration fails validation"""
    pass
```

### Error Messages

1. **Invalid Column**:
   ```
   IndexValidationError: Index column 'invalid_col' not found in table 'FUND_DATA'. 
   Available columns: ['id', 'fund_name', 'date', 'value']
   ```

2. **Multiple Invalid Columns**:
   ```
   IndexValidationError: Index columns ['col1', 'col2'] not found in table 'FUND_DATA'.
   Available columns: ['id', 'fund_name', 'date', 'value']
   ```

### Logging

- **INFO**: Successful validation with list of columns to be indexed
- **WARNING**: Duplicate columns in index list
- **ERROR**: Validation failures with details
- **DEBUG**: DDL generation steps

Example log messages:
```
INFO: Validated indexes for table FUND_DATA: ['id', 'date']
INFO: Generated 2 index statements for table FUND_DATA
WARNING: Duplicate column 'id' in index list for table FUND_DATA, will create only once
ERROR: Index validation failed for table FUND_DATA: column 'invalid' not found
```

## Migration and Deployment

### Backward Compatibility

- Existing configurations without `indexes` field continue to work
- No changes required to existing YAML files
- New field is optional with safe default (empty list)

### Rollout Strategy

1. **Phase 1**: Deploy code with new functionality (no config changes)
2. **Phase 2**: Add indexes to high-priority tables in config
3. **Phase 3**: Monitor query performance improvements
4. **Phase 4**: Gradually add indexes to remaining tables

### Performance Considerations

- Index creation is fast for empty tables (during initial setup)
- For existing tables with data, indexes may take time to build
- Consider creating indexes during maintenance windows for large tables
- PostgreSQL supports `CREATE INDEX CONCURRENTLY` for production (future enhancement)

## Future Enhancements

Potential future improvements (out of scope for this feature):

1. **Composite Indexes**: Support multi-column indexes
   ```yaml
   indexes:
     - columns: ["col1", "col2"]
   ```

2. **Index Types**: Support other index types (GIN, GiST, BRIN)
   ```yaml
   indexes:
     - column: "json_data"
       type: "gin"
   ```

3. **Partial Indexes**: Support WHERE clauses
   ```yaml
   indexes:
     - column: "status"
       where: "status = 'active'"
   ```

4. **Concurrent Creation**: Use `CREATE INDEX CONCURRENTLY` for production
5. **Index Analysis**: Suggest indexes based on query patterns
