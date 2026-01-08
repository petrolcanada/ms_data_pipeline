# Requirements Document

## Introduction

This feature adds simple index configuration to the DDL generation system. The system currently extracts metadata from Snowflake and generates basic CREATE TABLE statements for PostgreSQL. This enhancement will allow users to specify a list of columns to index in the table configuration file (config/tables.yaml), and the DDL generator will automatically create single-column B-tree indexes for each specified column.

## Glossary

- **DDL_Generator**: The component that generates PostgreSQL CREATE TABLE statements from Snowflake metadata
- **Table_Configuration**: The YAML file (config/tables.yaml) that defines which tables to sync and their properties
- **Index**: A database structure that improves query performance by allowing faster data retrieval
- **B-Tree_Index**: The default PostgreSQL index type, suitable for equality and range queries
- **Metadata_Extractor**: The component that extracts table schemas from Snowflake

## Requirements

### Requirement 1: Index Column Configuration in YAML

**User Story:** As a database administrator, I want to specify which columns to index in the table configuration file, so that I can optimize query performance without manually editing DDL files.

#### Acceptance Criteria

1. WHEN a user adds an indexes section to a table configuration, THE Table_Configuration SHALL accept a list of column names
2. WHEN a user specifies column names in the indexes list, THE Table_Configuration SHALL accept any valid column name as a string
3. WHERE a user omits the indexes section, THE Table_Configuration SHALL treat it as an empty list
4. THE Table_Configuration SHALL accept indexes as an optional field in the postgres section of each table

### Requirement 2: Index DDL Generation

**User Story:** As a developer, I want the DDL generator to create index statements from the configuration, so that indexes are automatically created with the tables.

#### Acceptance Criteria

1. WHEN the DDL_Generator processes a table with index configuration, THE DDL_Generator SHALL generate one CREATE INDEX statement for each column in the indexes list
2. WHEN generating an index statement, THE DDL_Generator SHALL use the format: CREATE INDEX idx_{table}_{column} ON {schema}.{table} ({column})
3. WHEN generating index statements, THE DDL_Generator SHALL append them after the CREATE TABLE statement
4. WHEN a table has no index configuration, THE DDL_Generator SHALL generate only the CREATE TABLE statement without indexes
5. WHEN generating indexes, THE DDL_Generator SHALL use B-tree as the default index type

### Requirement 3: Configuration Validation

**User Story:** As a developer, I want the system to validate index configurations, so that I can catch configuration errors before DDL generation.

#### Acceptance Criteria

1. WHEN loading table configuration, THE Table_Configuration SHALL validate that all specified index columns exist in the table metadata
2. IF a specified index column does not exist in the table, THEN THE Table_Configuration SHALL raise a descriptive error indicating which column is invalid
3. WHEN validation succeeds, THE Table_Configuration SHALL log a confirmation message listing the columns that will be indexed
4. WHEN an empty indexes list is provided, THE Table_Configuration SHALL skip validation and generate no indexes

### Requirement 4: Backward Compatibility

**User Story:** As a system maintainer, I want existing table configurations to continue working, so that the new feature doesn't break existing functionality.

#### Acceptance Criteria

1. WHEN a table configuration omits the indexes section, THE DDL_Generator SHALL generate DDL without index statements
2. WHEN processing existing configurations, THE Table_Configuration SHALL not require the indexes section
3. THE DDL_Generator SHALL maintain the existing DDL format for tables without index configuration
