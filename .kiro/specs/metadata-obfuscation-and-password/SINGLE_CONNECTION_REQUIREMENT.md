# Single Connection Requirement for Multi-Table Operations

## Date: December 29, 2024

## Problem Statement

When using SSO authentication with Snowflake, each new connection requires manual browser authentication. Previously, the metadata extractor created a new connection for each table, which meant:

- **5 tables** = 5 SSO browser authentications
- **10 tables** = 10 SSO browser authentications
- Significant manual overhead and time waste

## Solution

Refactored the metadata extractor to **reuse a single Snowflake connection** for all tables in a batch operation.

## Implementation

### Before (Multiple Connections):
```python
def extract_all_configured_tables(self):
    for table_config in config["tables"]:
        # Creates NEW connection for each table ❌
        metadata = self.extract_table_metadata(
            database, schema, table
        )
```

### After (Single Connection):
```python
def extract_all_configured_tables(self):
    # Create ONE connection for all tables ✅
    conn = self.connect_to_snowflake()
    
    try:
        for table_config in config["tables"]:
            # Reuse existing connection
            metadata = self.extract_table_metadata(
                database, schema, table,
                conn=conn  # Pass connection
            )
    finally:
        conn.close()
```

## Changes Made

### 1. Updated `extract_table_metadata()` Method

**File:** `pipeline/extractors/metadata_extractor.py`

```python
def extract_table_metadata(
    self, 
    database: str, 
    schema: str, 
    table: str, 
    conn=None  # NEW: Optional connection parameter
) -> Dict[str, Any]:
    """
    Extract complete metadata for a specific table
    
    Args:
        database: Snowflake database name
        schema: Snowflake schema name
        table: Snowflake table name
        conn: Optional existing Snowflake connection
              If None, creates new connection
    """
    # Use provided connection or create new one
    should_close = False
    if conn is None:
        conn = self.connect_to_snowflake()
        should_close = True
    
    cursor = conn.cursor()
    
    try:
        # ... extraction logic ...
        return metadata
    finally:
        cursor.close()
        # Only close connection if we created it
        if should_close:
            conn.close()
```

### 2. Updated `extract_all_configured_tables()` Method

**File:** `pipeline/extractors/metadata_extractor.py`

```python
def extract_all_configured_tables(self, ...):
    # Load table configuration
    with open("config/tables.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    # Create single Snowflake connection for all tables
    logger.info("Establishing Snowflake connection for all tables...")
    conn = self.connect_to_snowflake()
    
    try:
        for table_config in config["tables"]:
            # Extract metadata (reuse connection)
            metadata = self.extract_table_metadata(
                sf_config["database"],
                sf_config["schema"],
                sf_config["table"],
                conn=conn  # Pass existing connection
            )
            # ... rest of processing ...
    finally:
        # Close the shared connection
        logger.info("Closing Snowflake connection")
        conn.close()
```

## Benefits

### 1. Single SSO Authentication
- **Before**: 10 tables = 10 browser authentications
- **After**: 10 tables = 1 browser authentication
- **Time saved**: ~90% reduction in authentication overhead

### 2. Faster Execution
- No connection overhead between tables
- Reduced network latency
- Connection pooling benefits

### 3. Better Resource Management
- Single connection = less memory usage
- Cleaner connection lifecycle
- Easier to debug connection issues

### 4. Improved User Experience
- User authenticates once at the start
- No interruptions during processing
- More predictable execution time

## Data Export Already Optimized

The data export script (`scripts/export_data.py`) already uses this pattern via `SnowflakeConnectionManager`:

```python
# Single connection for all tables
with SnowflakeConnectionManager() as conn_manager:
    print("✅ Connected to Snowflake (SSO authentication complete)")
    
    for table_config in config['tables']:
        export_result = export_table(
            table_config,
            password,
            export_base_dir,
            conn_manager,  # Reuses connection
            ...
        )
```

## Going Forward: Mandatory Requirement

### Requirement

**ALL batch operations that process multiple tables MUST reuse a single Snowflake connection.**

This applies to:
- ✅ Metadata extraction (`extract_all_configured_tables`)
- ✅ Data export (`export_data.py` with `SnowflakeConnectionManager`)
- ⚠️ Any future multi-table operations

### Design Pattern

When implementing multi-table operations:

1. **Create connection once** at the start of batch operation
2. **Pass connection** to individual table processing functions
3. **Close connection** in finally block after all tables processed
4. **Make connection parameter optional** to support single-table operations

### Code Template

```python
def process_all_tables(self, ...):
    """Process all configured tables"""
    
    # Load configuration
    config = load_config()
    
    # Create single connection for all tables
    logger.info("Establishing Snowflake connection...")
    conn = self.connect_to_snowflake()
    
    try:
        for table_config in config["tables"]:
            # Process table with shared connection
            result = self.process_single_table(
                table_config,
                conn=conn  # Pass connection
            )
    finally:
        # Always close connection
        logger.info("Closing Snowflake connection")
        conn.close()

def process_single_table(self, table_config, conn=None):
    """Process a single table"""
    
    # Support both shared and new connections
    should_close = False
    if conn is None:
        conn = self.connect_to_snowflake()
        should_close = True
    
    try:
        # ... processing logic ...
        return result
    finally:
        # Only close if we created it
        if should_close:
            conn.close()
```

## Testing

To verify single connection behavior:

```bash
# Test with multiple tables
python scripts/extract_metadata.py --all

# Expected behavior:
# 1. Single SSO browser authentication at start
# 2. "Establishing Snowflake connection for all tables..." message
# 3. All tables processed without additional authentication
# 4. "Closing Snowflake connection" message at end
```

## Impact

### Metadata Extraction
- **Before**: N tables = N SSO authentications
- **After**: N tables = 1 SSO authentication
- **Improvement**: ~(N-1)/N reduction in authentication overhead

### Example with 10 Tables
- **Before**: ~5 minutes (10 × 30 seconds per auth)
- **After**: ~30 seconds (1 × 30 seconds auth)
- **Time saved**: 4.5 minutes (90% reduction)

## Related Files

- `pipeline/extractors/metadata_extractor.py` - Updated for single connection
- `scripts/extract_metadata.py` - Uses updated extractor
- `scripts/export_data.py` - Already uses single connection pattern
- `pipeline/connections/snowflake_connection.py` - Connection manager

## Notes

- This is especially important for SSO authentication
- Also benefits password and key-pair authentication (faster)
- Connection is closed in finally block (ensures cleanup)
- Individual table methods still support creating their own connection (backward compatible)
- This pattern should be used for ALL future multi-table operations
