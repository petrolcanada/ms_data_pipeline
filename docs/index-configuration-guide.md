# PostgreSQL Index Configuration Guide

## Overview

The PostgreSQL index configuration feature allows you to specify which columns should be indexed when creating tables in PostgreSQL. This improves query performance for frequently accessed columns.

## Configuration

### Adding Indexes to a Table

Edit `config/tables.yaml` and add an `indexes` field under the `postgres` section:

```yaml
- name: "YOUR_TABLE_NAME"
  snowflake:
    database: "YOUR_DATABASE"
    schema: "YOUR_SCHEMA"
    table: "YOUR_TABLE"
  postgres:
    schema: "target_schema"
    table: "target_table"
    indexes:  # Add this section
      - "COLUMN1"
      - "COLUMN2"
      - "COLUMN3"
```

### Example Configuration

```yaml
- name: "FUND_ATTRIBUTES_CA_OPENEND"
  snowflake:
    database: "CIGAM_PRD_RL"
    schema: "MORNINGSTAR_MAIN"
    table: "FUND_ATTRIBUTES_CA_OPENEND"
    filter:
      - "WHERE _ID IN (SELECT mstarid FROM ...)"
      - "QUALIFY ROW_NUMBER() OVER (PARTITION BY _ID ORDER BY _TIMESTAMPTO DESC) = 1"
  postgres:
    schema: "ms"
    table: "FUND_ATTRIBUTES_CA_OPENEND"
    indexes:
      - "_ID"
      - "_TIMESTAMPFROM"
      - "_TIMESTAMPTO"
```

## Index Selection Guidelines

### 1. Partition Columns from QUALIFY Clauses

If your Snowflake query uses a QUALIFY clause with PARTITION BY, index those columns:

```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY _ID, MONTHENDDATE ORDER BY _TIMESTAMPTO DESC) = 1
```

Index these columns:
```yaml
indexes:
  - "_ID"
  - "MONTHENDDATE"
```

### 2. Temporal Columns

For tables with temporal tracking, index timestamp columns:

```yaml
indexes:
  - "_TIMESTAMPFROM"
  - "_TIMESTAMPTO"
```

### 3. Foreign Keys and Join Columns

Index columns frequently used in JOINs:

```yaml
indexes:
  - "FUND_ID"
  - "ACCOUNT_ID"
```

### 4. Frequently Filtered Columns

Index columns used in WHERE clauses:

```yaml
indexes:
  - "STATUS"
  - "CATEGORY"
  - "DATE"
```

## Index Types

Currently, the system creates **B-tree indexes** (PostgreSQL default), which are suitable for:
- Equality comparisons (`=`)
- Range queries (`<`, `>`, `BETWEEN`)
- Sorting operations (`ORDER BY`)
- Pattern matching with leading characters (`LIKE 'prefix%'`)

## Generated Index Names

Indexes follow the naming convention: `idx_{table}_{column}`

Examples:
- Table: `FUND_DATA`, Column: `_ID` → Index: `idx_FUND_DATA__ID`
- Table: `MARKET_PRICES`, Column: `DATE` → Index: `idx_MARKET_PRICES_DATE`

## Validation

The system validates index configurations before generating DDL:

### Valid Configuration
```yaml
indexes:
  - "_ID"          # Column exists in table
  - "MONTHENDDATE" # Column exists in table
```
✅ Validation passes, indexes will be created

### Invalid Configuration
```yaml
indexes:
  - "_ID"            # Column exists
  - "INVALID_COLUMN" # Column does NOT exist
```
❌ Validation fails with error:
```
IndexValidationError: Index column(s) ['INVALID_COLUMN'] not found in table 'FUND_DATA'. 
Available columns: ['_ID', 'MONTHENDDATE', '_TIMESTAMPFROM', '_TIMESTAMPTO', 'VALUE']
```

### Duplicate Columns
```yaml
indexes:
  - "_ID"
  - "_ID"  # Duplicate
  - "DATE"
```
⚠️ Warning logged, but only one index created for `_ID`

## Usage Workflow

### 1. Update Configuration

Edit `config/tables.yaml` to add indexes:

```yaml
postgres:
  schema: "ms"
  table: "my_table"
  indexes:
    - "column1"
    - "column2"
```

### 2. Extract Metadata and Generate DDL

Run the metadata extraction script:

```bash
python scripts/extract_metadata.py
```

This will:
- Extract table metadata from Snowflake
- Validate index configuration
- Generate DDL with CREATE INDEX statements
- Save DDL to `metadata/ddl/`

### 3. Create Tables in PostgreSQL

Run the table creation script:

```bash
python scripts/create_tables.py
```

Or use the PostgreSQL loader directly:

```python
from pipeline.loaders.postgres_loader import PostgreSQLLoader

loader = PostgreSQLLoader()
loader.create_table_from_metadata("TABLE_NAME", drop_if_exists=True)
```

### 4. Verify Indexes

Connect to PostgreSQL and check indexes:

```sql
-- List all indexes for a table
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE schemaname = 'ms' 
  AND tablename = 'your_table_name';

-- Check index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'ms'
ORDER BY idx_scan DESC;
```

## Backward Compatibility

### Tables Without Indexes

If you omit the `indexes` field, the table will be created without custom indexes:

```yaml
postgres:
  schema: "ms"
  table: "my_table"
  # No indexes field - this is valid
```

This is fully backward compatible with existing configurations.

### Empty Index List

An empty index list is also valid:

```yaml
postgres:
  schema: "ms"
  table: "my_table"
  indexes: []  # Explicitly no indexes
```

## Performance Considerations

### Benefits
- **Faster lookups**: Queries filtering by indexed columns are much faster
- **Improved joins**: JOIN operations on indexed columns are optimized
- **Better sorting**: ORDER BY on indexed columns is faster

### Trade-offs
- **Storage overhead**: Indexes consume disk space
- **Write performance**: INSERT/UPDATE/DELETE operations are slightly slower
- **Maintenance**: Indexes need to be maintained by PostgreSQL

### Best Practices
1. **Index selectively**: Don't index every column
2. **Monitor usage**: Use `pg_stat_user_indexes` to check if indexes are used
3. **Consider cardinality**: High-cardinality columns benefit more from indexes
4. **Test performance**: Measure query performance before and after indexing

## Troubleshooting

### Error: "Index column not found"

**Problem**: Column name in `indexes` doesn't match table metadata

**Solution**: 
1. Check column name spelling (case-sensitive)
2. Verify column exists in Snowflake table
3. Run metadata extraction to get current column list

### Error: "Metadata file not found"

**Problem**: DDL generation requires metadata file

**Solution**: Run metadata extraction first:
```bash
python scripts/extract_metadata.py
```

### Indexes Not Created

**Problem**: Tables created but indexes missing

**Solution**:
1. Check DDL file contains CREATE INDEX statements
2. Verify no PostgreSQL errors during table creation
3. Check PostgreSQL logs for index creation errors

## Examples

### Example 1: Simple Table with ID Index

```yaml
- name: "USERS"
  snowflake:
    database: "PROD_DB"
    schema: "PUBLIC"
    table: "USERS"
  postgres:
    schema: "public"
    table: "users"
    indexes:
      - "USER_ID"
```

### Example 2: Time-Series Data

```yaml
- name: "MARKET_PRICES"
  snowflake:
    database: "PROD_DB"
    schema: "MARKET"
    table: "DAILY_PRICES"
  postgres:
    schema: "market"
    table: "daily_prices"
    indexes:
      - "SYMBOL"
      - "PRICE_DATE"
      - "EXCHANGE"
```

### Example 3: Partitioned Table

```yaml
- name: "TRANSACTIONS"
  snowflake:
    database: "PROD_DB"
    schema: "FINANCE"
    table: "TRANSACTIONS"
    filter:
      - "QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID, TRANSACTION_DATE ORDER BY UPDATED_AT DESC) = 1"
  postgres:
    schema: "finance"
    table: "transactions"
    indexes:
      - "ACCOUNT_ID"
      - "TRANSACTION_DATE"
      - "UPDATED_AT"
```

## Future Enhancements

Potential future improvements (not currently implemented):

1. **Composite indexes**: Multi-column indexes
2. **Index types**: GIN, GiST, BRIN indexes
3. **Partial indexes**: Indexes with WHERE clauses
4. **Concurrent creation**: `CREATE INDEX CONCURRENTLY`
5. **Index suggestions**: Automatic index recommendations based on query patterns

## Related Documentation

- [Command Reference](command-reference.md)
- [Complete Workflow](complete-workflow.md)
- [Metadata Workflow Guide](metadata-workflow-guide.md)
