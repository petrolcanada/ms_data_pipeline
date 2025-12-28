# Requirements Document

## Introduction

This feature adds SQL filtering capability to the data extraction process, allowing users to extract only specific subsets of data from Snowflake tables based on WHERE clauses, subqueries, or other SQL filters. This is essential for reducing data transfer volumes and extracting only relevant records.

## Glossary

- **Data_Extractor**: The component that extracts data from Snowflake tables
- **Filter_Clause**: A SQL WHERE clause or condition that limits the rows extracted
- **Table_Configuration**: YAML configuration defining table sync settings
- **Subquery**: A nested SELECT statement used within a filter condition
- **Export_Script**: The script that orchestrates data export from Snowflake

## Requirements

### Requirement 1: Optional Filter Configuration

**User Story:** As a data engineer, I want to optionally specify SQL filters for each table, so that I can extract only the data I need without pulling entire tables.

#### Acceptance Criteria

1. WHEN a table configuration includes a filter clause, THE Data_Extractor SHALL apply that filter during data extraction
2. WHEN a table configuration does not include a filter clause, THE Data_Extractor SHALL extract all rows from the table
3. THE Table_Configuration SHALL support an optional "filter" field under the snowflake section
4. WHEN a filter is specified as a string, THE Data_Extractor SHALL validate that it is non-empty
5. WHEN a filter is specified as a list, THE Data_Extractor SHALL validate that all items are non-empty strings

### Requirement 2: WHERE Clause Support

**User Story:** As a data engineer, I want to use standard SQL WHERE clauses in my filters, so that I can leverage familiar SQL syntax for filtering data.

#### Acceptance Criteria

1. WHEN a filter starts with "WHERE", THE Data_Extractor SHALL append it directly to the SELECT statement
2. WHEN a filter does not start with "WHERE", THE Data_Extractor SHALL prepend "WHERE" automatically
3. THE Data_Extractor SHALL preserve the exact filter syntax provided by the user
4. WHEN the filter contains syntax errors, THE Data_Extractor SHALL propagate the Snowflake error message to the user

### Requirement 3: Subquery Support

**User Story:** As a data engineer, I want to use subqueries in my filters, so that I can extract data based on relationships with other tables or views.

#### Acceptance Criteria

1. WHEN a filter contains a subquery, THE Data_Extractor SHALL execute it as part of the main query
2. THE Data_Extractor SHALL support subqueries with IN, EXISTS, and comparison operators
3. WHEN a subquery references tables in different schemas, THE Data_Extractor SHALL use fully qualified table names
4. WHEN a subquery fails, THE Data_Extractor SHALL report the error with the full query context

### Requirement 4: Filter Application in Data Export

**User Story:** As a data engineer, I want filters to apply during data export, so that only filtered data is encrypted and transferred.

#### Acceptance Criteria

1. WHEN exporting data with a filter, THE Export_Script SHALL pass the filter to the Data_Extractor
2. THE Data_Extractor SHALL apply the filter before chunking the data
3. WHEN estimating table size with a filter, THE Data_Extractor SHALL count only filtered rows
4. THE manifest file SHALL record whether a filter was applied during export

### Requirement 5: Filter Validation and Error Handling

**User Story:** As a data engineer, I want clear error messages when my filter is invalid, so that I can quickly fix configuration issues.

#### Acceptance Criteria

1. WHEN a filter causes a SQL error, THE Data_Extractor SHALL display the full error message from Snowflake
2. WHEN a filter references non-existent columns, THE Data_Extractor SHALL report the column name in the error
3. WHEN a filter references non-existent tables, THE Data_Extractor SHALL report the table name in the error
4. THE Data_Extractor SHALL log the complete SQL query being executed for debugging purposes

### Requirement 6: Backward Compatibility

**User Story:** As a data engineer, I want existing table configurations without filters to continue working, so that I don't need to update all configurations immediately.

#### Acceptance Criteria

1. WHEN a table configuration omits the filter field, THE Data_Extractor SHALL extract all rows
2. WHEN a table configuration has an empty filter field, THE Data_Extractor SHALL extract all rows
3. WHEN a table configuration has a null filter field, THE Data_Extractor SHALL extract all rows
4. THE Export_Script SHALL work with both old and new configuration formats

### Requirement 7: Filter Documentation in Manifest

**User Story:** As a data engineer, I want the export manifest to document which filter was used, so that I can verify what data was extracted.

#### Acceptance Criteria

1. WHEN a filter is applied, THE manifest file SHALL include the filter clause in the metadata
2. WHEN no filter is applied, THE manifest file SHALL indicate "no filter" or omit the filter field
3. THE manifest file SHALL record the estimated row count before and after filtering
4. WHEN importing data, THE import process SHALL display the filter information from the manifest

### Requirement 8: Multiple Filter Conditions

**User Story:** As a data engineer, I want to specify multiple filter conditions that are combined together, so that I can apply complex filtering logic without writing one long WHERE clause.

#### Acceptance Criteria

1. WHEN a filter is specified as a list of strings, THE Data_Extractor SHALL combine them with AND operators
2. WHEN a filter is specified as a single string, THE Data_Extractor SHALL use it as-is
3. WHEN combining multiple filters, THE Data_Extractor SHALL preserve the order specified in the configuration
4. WHEN a filter item starts with "AND", THE Data_Extractor SHALL use it directly in the combination
5. WHEN a filter item does not start with "AND" or "WHERE", THE Data_Extractor SHALL prepend "AND" automatically
6. THE Data_Extractor SHALL support filters with LIKE, IN, BETWEEN, and other SQL operators in each condition

### Requirement 9: Complex Filter Support

**User Story:** As a data engineer, I want to use complex SQL expressions in my filters, so that I can leverage the full power of SQL for data extraction.

#### Acceptance Criteria

1. THE Data_Extractor SHALL support filters with AND, OR, and NOT operators within individual conditions
2. THE Data_Extractor SHALL support filters with parentheses for grouping conditions
3. THE Data_Extractor SHALL support filters with date functions and comparisons
4. THE Data_Extractor SHALL support filters with CASE statements and other SQL expressions


## Configuration Examples

### Example 1: Single Filter Condition
```yaml
tables:
  - name: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    snowflake:
      database: "CIGAM_PRD_RL"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
      filter: "WHERE SECID IN (SELECT SECID FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.VW_ACTIVE_FUNDS)"
    postgres:
      schema: "ms"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

**Generated SQL:**
```sql
SELECT * FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
WHERE SECID IN (SELECT SECID FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.VW_ACTIVE_FUNDS)
```

### Example 2: Multiple Filter Conditions (List Format)
```yaml
tables:
  - name: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    snowflake:
      database: "CIGAM_PRD_RL"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
      filter:
        - "WHERE SECID IN (SELECT SECID FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.VW_ACTIVE_FUNDS)"
        - "AND _TIMESTAMPTO LIKE '%9999%'"
    postgres:
      schema: "ms"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

**Generated SQL:**
```sql
SELECT * FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
WHERE SECID IN (SELECT SECID FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.VW_ACTIVE_FUNDS)
AND _TIMESTAMPTO LIKE '%9999%'
```

### Example 3: Multiple Conditions Without "AND" Prefix
```yaml
tables:
  - name: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    snowflake:
      database: "CIGAM_PRD_RL"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
      filter:
        - "SECID IN (SELECT SECID FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.VW_ACTIVE_FUNDS)"
        - "_TIMESTAMPTO LIKE '%9999%'"
        - "STATUS = 'ACTIVE'"
    postgres:
      schema: "ms"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

**Generated SQL:**
```sql
SELECT * FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
WHERE SECID IN (SELECT SECID FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.VW_ACTIVE_FUNDS)
AND _TIMESTAMPTO LIKE '%9999%'
AND STATUS = 'ACTIVE'
```

### Example 4: Complex Filter with OR Logic
```yaml
tables:
  - name: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    snowflake:
      database: "CIGAM_PRD_RL"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
      filter: "WHERE (SECID IN (SELECT SECID FROM VW_ACTIVE_FUNDS) OR STATUS = 'PENDING') AND _TIMESTAMPTO LIKE '%9999%'"
    postgres:
      schema: "ms"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

**Generated SQL:**
```sql
SELECT * FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
WHERE (SECID IN (SELECT SECID FROM VW_ACTIVE_FUNDS) OR STATUS = 'PENDING') AND _TIMESTAMPTO LIKE '%9999%'
```

### Example 5: No Filter (Extract All Data)
```yaml
tables:
  - name: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    snowflake:
      database: "CIGAM_PRD_RL"
      schema: "MORNINGSTAR_MAIN"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
    postgres:
      schema: "ms"
      table: "FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND"
```

**Generated SQL:**
```sql
SELECT * FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND
```
