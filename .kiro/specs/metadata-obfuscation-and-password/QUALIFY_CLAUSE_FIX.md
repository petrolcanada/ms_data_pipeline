# QUALIFY Clause Support Fix

## Date: December 29, 2024

## Problem

The `QUALIFY` clause is a Snowflake-specific SQL feature that must come **after** the `WHERE` clause, not be joined with `AND`. The previous implementation was incorrectly treating `QUALIFY` as just another WHERE condition, resulting in invalid SQL:

```sql
-- ❌ INCORRECT (what was happening)
SELECT * FROM table
WHERE _ID IN (SELECT ...)
AND QUALIFY ROW_NUMBER() OVER (...) = 1
-- Syntax error: QUALIFY cannot be joined with AND
```

The correct SQL should be:

```sql
-- ✅ CORRECT (what should happen)
SELECT * FROM table
WHERE _ID IN (SELECT ...)
QUALIFY ROW_NUMBER() OVER (...) = 1
-- QUALIFY is a separate clause after WHERE
```

## Solution

Updated the `_build_filter_clause()` method in `pipeline/extractors/data_extractor.py` to:

1. **Detect QUALIFY clauses** - Identify filters that start with "QUALIFY"
2. **Separate WHERE and QUALIFY** - Keep them in separate lists
3. **Build correct SQL** - Append QUALIFY after WHERE (not with AND)

## Implementation

### Updated Method Logic

```python
def _build_filter_clause(self, filter_config) -> str:
    # Separate WHERE conditions from QUALIFY clauses
    where_conditions = []
    qualify_clauses = []
    
    for filter_item in filters:
        if filter_item.upper().startswith("QUALIFY"):
            qualify_clauses.append(filter_item)
        else:
            where_conditions.append(filter_item)
    
    # Build WHERE clause first
    result = build_where_clause(where_conditions)
    
    # Add QUALIFY clauses after WHERE (not with AND)
    for qualify_clause in qualify_clauses:
        result += f" {qualify_clause}"
    
    return result
```

## YAML Configuration Example

Now you can use QUALIFY in your table configuration:

```yaml
- name: "MONTH_END_TRAILING_TOTAL_RETURNS_CA_OPENEND"
  snowflake:
    database: "CIGAM_PRD_RL"
    schema: "MORNINGSTAR_MAIN"
    table: "MONTH_END_TRAILING_TOTAL_RETURNS_CA_OPENEND"
    filter:
      - "WHERE _ID IN (SELECT mstarid FROM ...)"
      - "QUALIFY ROW_NUMBER() OVER (PARTITION BY _ID, MONTHENDDATE ORDER BY _TIMESTAMPTO DESC) = 1"
  postgres:
    schema: "ms"
    table: "MONTH_END_TRAILING_TOTAL_RETURNS_CA_OPENEND"
```

## Generated SQL

The above configuration will generate:

```sql
SELECT * 
FROM CIGAM_PRD_RL.MORNINGSTAR_MAIN.MONTH_END_TRAILING_TOTAL_RETURNS_CA_OPENEND
WHERE _ID IN (SELECT mstarid FROM CIGAM_SBX_PL.DNA_ANALYTICS_INSIGHTS_INVESTMENT_PRODUCT.V_MSTAR_PEERS_FCLASS_ETF_ID_CA_OPENEND)
QUALIFY ROW_NUMBER() OVER (PARTITION BY _ID, MONTHENDDATE ORDER BY _TIMESTAMPTO DESC) = 1
```

## How It Works

### 1. Multiple WHERE Conditions (Joined with AND)

```yaml
filter:
  - "WHERE condition1"
  - "AND condition2"
  - "AND condition3"
```

Result:
```sql
WHERE condition1 AND condition2 AND condition3
```

### 2. WHERE + QUALIFY (Separate Clauses)

```yaml
filter:
  - "WHERE condition1"
  - "AND condition2"
  - "QUALIFY window_function() = 1"
```

Result:
```sql
WHERE condition1 AND condition2
QUALIFY window_function() = 1
```

### 3. Multiple QUALIFY Clauses

```yaml
filter:
  - "WHERE condition1"
  - "QUALIFY window_function1() = 1"
  - "QUALIFY window_function2() = 1"
```

Result:
```sql
WHERE condition1
QUALIFY window_function1() = 1
QUALIFY window_function2() = 1
```

## Testing

To test the fix:

```bash
# Run data export with QUALIFY clause
python scripts/export_data.py --table MONTH_END_TRAILING_TOTAL_RETURNS_CA_OPENEND

# Check the generated SQL in logs
# Should see:
# Query: SELECT * FROM ... WHERE ... QUALIFY ...
```

## Benefits

✅ **Correct SQL syntax** - QUALIFY is properly placed after WHERE
✅ **Flexible configuration** - Mix WHERE and QUALIFY clauses freely
✅ **Snowflake-specific** - Supports Snowflake's window function filtering
✅ **Backward compatible** - Existing WHERE-only filters still work

## Use Cases

### Deduplication

Get the latest record per group:

```yaml
filter:
  - "WHERE active = true"
  - "QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1"
```

### Top N per Group

Get top 3 records per category:

```yaml
filter:
  - "WHERE status = 'active'"
  - "QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) <= 3"
```

### Filtering by Window Function

Get records above average:

```yaml
filter:
  - "WHERE year = 2024"
  - "QUALIFY value > AVG(value) OVER (PARTITION BY region)"
```

## Notes

- `QUALIFY` is Snowflake-specific (not standard SQL)
- `QUALIFY` must come after `WHERE` in the query
- Multiple `QUALIFY` clauses are allowed
- `QUALIFY` can use any window function (ROW_NUMBER, RANK, etc.)
- The fix maintains backward compatibility with existing configurations

## Related Files

- `pipeline/extractors/data_extractor.py` - Updated `_build_filter_clause()` method
- `config/tables.yaml` - Example configuration with QUALIFY clause
