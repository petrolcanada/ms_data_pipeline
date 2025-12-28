# Connection Management Architecture

This document explains the centralized connection management system.

---

## **Overview**

The system uses dedicated connection manager classes to handle database connections efficiently and securely.

### **Key Benefits:**
✅ **Single SSO Authentication** - Connect once, reuse for all operations
✅ **Automatic Cleanup** - Context managers ensure connections are closed
✅ **Connection Reuse** - Better performance across multiple operations
✅ **Centralized Logic** - All connection code in one place
✅ **Easier Testing** - Mock connection managers instead of databases

---

## **Architecture**

```
pipeline/connections/
├── __init__.py                    # Module exports
├── base_connection.py             # Abstract base class
├── snowflake_connection.py        # Snowflake connection manager
└── postgres_connection.py         # PostgreSQL connection manager
```

---

## **Connection Managers**

### **SnowflakeConnectionManager**

Manages Snowflake connections with SSO support.

**Features:**
- Single SSO authentication per session
- Connection reuse across operations
- Automatic reconnection on connection loss
- Context manager support

**Usage:**
```python
from pipeline.connections import SnowflakeConnectionManager

# Single operation
with SnowflakeConnectionManager() as conn_mgr:
    conn = conn_mgr.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table")
    # Connection automatically closed

# Multiple operations (recommended)
with SnowflakeConnectionManager() as conn_mgr:
    extractor = SnowflakeDataExtractor(conn_mgr)
    extractor.estimate_table_size(...)  # Uses connection
    extractor.extract_table_chunks(...)  # Reuses same connection
# Connection automatically closed
```

### **PostgresConnectionManager**

Manages PostgreSQL connections with transaction support.

**Features:**
- Connection reuse across operations
- Transaction management (commit/rollback)
- Autocommit mode support
- Context manager support

**Usage:**
```python
from pipeline.connections import PostgresConnectionManager

# With transactions
with PostgresConnectionManager() as conn_mgr:
    loader = PostgreSQLDataLoader(conn_mgr)
    loader.load_parquet_to_table(...)
    conn_mgr.commit()  # Explicit commit

# With autocommit
with PostgresConnectionManager(autocommit=True) as conn_mgr:
    loader = PostgreSQLDataLoader(conn_mgr)
    loader.load_parquet_to_table(...)  # Auto-committed
```

---

## **How It Works**

### **Before (Multiple Connections)**

```python
# Old approach - creates 2 connections (2 SSO prompts)
extractor = SnowflakeDataExtractor()
extractor.estimate_table_size(...)  # Connection #1 (SSO prompt #1)
extractor.extract_table_chunks(...)  # Connection #2 (SSO prompt #2)
```

### **After (Single Connection)**

```python
# New approach - creates 1 connection (1 SSO prompt)
with SnowflakeConnectionManager() as conn_mgr:
    extractor = SnowflakeDataExtractor(conn_mgr)
    extractor.estimate_table_size(...)  # Uses connection
    extractor.extract_table_chunks(...)  # Reuses same connection
# Connection automatically closed
```

---

## **Script Integration**

### **Export Script (export_data.py)**

```python
# Single SSO authentication for all tables
with SnowflakeConnectionManager() as conn_manager:
    print("✅ Connected to Snowflake (SSO authentication complete)")
    
    for table_config in tables:
        export_table(table_config, password, export_base_dir, conn_manager)
# Connection automatically closed
```

**Benefits:**
- ✅ One SSO prompt for all tables
- ✅ Faster multi-table exports
- ✅ Automatic connection cleanup

### **Import Script (import_data.py)**

```python
# Single PostgreSQL connection for all tables
with PostgresConnectionManager() as conn_manager:
    for table_config in tables:
        import_table(table_config, password, import_base_dir, conn_manager)
# Connection automatically closed
```

**Benefits:**
- ✅ Connection reuse across tables
- ✅ Better performance
- ✅ Automatic cleanup

---

## **Connection Lifecycle**

### **1. Creation**
```python
with SnowflakeConnectionManager() as conn_mgr:
    # Connection created on first get_connection() call
    conn = conn_mgr.get_connection()
```

### **2. Reuse**
```python
    # Subsequent calls reuse the same connection
    conn = conn_mgr.get_connection()  # Returns existing connection
```

### **3. Health Check**
```python
    # Automatically checks if connection is alive
    if not conn_mgr.is_alive():
        # Reconnects automatically
        conn = conn_mgr.get_connection()
```

### **4. Cleanup**
```python
# Connection automatically closed when exiting context
# No manual cleanup needed
```

---

## **Error Handling**

### **Connection Failures**

```python
try:
    with SnowflakeConnectionManager() as conn_mgr:
        # Operations
        pass
except Exception as e:
    # Connection automatically closed even on error
    logger.error(f"Operation failed: {e}")
```

### **Automatic Reconnection**

```python
with SnowflakeConnectionManager() as conn_mgr:
    # First operation
    conn = conn_mgr.get_connection()
    
    # If connection dies, automatically reconnects
    conn = conn_mgr.get_connection()  # Checks health, reconnects if needed
```

---

## **Testing**

### **Mock Connection Manager**

```python
class MockSnowflakeConnectionManager:
    def get_connection(self):
        return MockConnection()
    
    def close(self):
        pass

# Use in tests
extractor = SnowflakeDataExtractor(MockSnowflakeConnectionManager())
```

---

## **Best Practices**

### **1. Always Use Context Managers**

✅ **Good:**
```python
with SnowflakeConnectionManager() as conn_mgr:
    # Operations
    pass
# Connection automatically closed
```

❌ **Avoid:**
```python
conn_mgr = SnowflakeConnectionManager()
conn = conn_mgr.get_connection()
# ... operations ...
conn_mgr.close()  # Easy to forget!
```

### **2. Reuse Connection Managers**

✅ **Good:**
```python
with SnowflakeConnectionManager() as conn_mgr:
    extractor = SnowflakeDataExtractor(conn_mgr)
    extractor.estimate_table_size(...)
    extractor.extract_table_chunks(...)  # Reuses connection
```

❌ **Avoid:**
```python
extractor1 = SnowflakeDataExtractor()  # Creates connection #1
extractor2 = SnowflakeDataExtractor()  # Creates connection #2
```

### **3. Pass Connection Managers to Functions**

✅ **Good:**
```python
def export_table(table_config, conn_manager):
    extractor = SnowflakeDataExtractor(conn_manager)
    # Operations

with SnowflakeConnectionManager() as conn_mgr:
    for table in tables:
        export_table(table, conn_mgr)  # Reuses connection
```

❌ **Avoid:**
```python
def export_table(table_config):
    extractor = SnowflakeDataExtractor()  # Creates new connection each time
    # Operations

for table in tables:
    export_table(table)  # New connection per table
```

---

## **Migration Guide**

### **Updating Existing Code**

**Before:**
```python
extractor = SnowflakeDataExtractor()
extractor.estimate_table_size(...)
```

**After:**
```python
with SnowflakeConnectionManager() as conn_mgr:
    extractor = SnowflakeDataExtractor(conn_mgr)
    extractor.estimate_table_size(...)
```

### **Backward Compatibility**

The extractors/loaders still work without connection managers:

```python
# Still works (creates own connection manager)
extractor = SnowflakeDataExtractor()
extractor.estimate_table_size(...)
```

But you'll see a warning:
```
WARNING: No connection manager provided - creating new one. 
Consider passing a connection manager for better performance.
```

---

## **Performance Impact**

### **Single Table Export**

**Before:**
- 2 SSO authentications
- 2 connections created/closed
- ~30 seconds overhead

**After:**
- 1 SSO authentication
- 1 connection created/closed
- ~15 seconds overhead

**Savings:** 50% reduction in connection overhead

### **Multiple Table Export (5 tables)**

**Before:**
- 10 SSO authentications (2 per table)
- 10 connections created/closed
- ~150 seconds overhead

**After:**
- 1 SSO authentication (shared)
- 1 connection created/closed
- ~15 seconds overhead

**Savings:** 90% reduction in connection overhead

---

## **Summary**

| Feature | Before | After |
|---------|--------|-------|
| SSO Prompts (1 table) | 2 | 1 |
| SSO Prompts (5 tables) | 10 | 1 |
| Connection Reuse | ❌ | ✅ |
| Auto Cleanup | ❌ | ✅ |
| Centralized Logic | ❌ | ✅ |
| Easy Testing | ❌ | ✅ |

**Key Takeaway:** Use connection managers with context managers for optimal performance and resource management.
