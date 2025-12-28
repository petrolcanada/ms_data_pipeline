`DATABASE.SCHEMA.VIEW_NAME`

### **Error: Syntax error**

**Problem:**
```
SQL compilation error: syntax error line 1 at position 45
```

**Solution:** Test your filter in Snowflake first. Check for:
- Missing quotes around strings
- Unmatched parentheses
- Invalid SQL syntax

### **No Data Extracted**

**Problem:** Export completes but 0 rows extracted.

**Solution:** 
1. Verify filter returns data in Snowflake
2. Check filter logic (AND vs OR)
3. Verify subquery returns results

---

## **Command Reference**

### **Export with Filter**

```bash
# Export single table (uses filter from config)
python scripts/export_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Export all tables (uses filters from config)
python scripts/export_data.py --all
```

### **Import Filtered Data**

```bash
# Import single table
python scripts/import_data.py --table FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND

# Import all tables
python scripts/import_data.py --all
```

### **Verify Filter in Manifest**

```bash
# Check what filter was used
cat D:/snowflake_exports/FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND/manifest.json | grep filter
```

---

## **Summary**

| Feature | Description |
|---------|-------------|
| **Format** | String or list of conditions |
| **Syntax** | Standard SQL WHERE clauses |
| **Operators** | IN, LIKE, =, >, <, BETWEEN, AND, OR |
| **Subqueries** | Fully supported |
| **Optional** | Yes - omit for full table export |
| **Documentation** | Recorded in manifest.json |
| **Backward Compatible** | Yes - existing configs work unchanged |

**Key Takeaway:** Filters reduce data volume, speed up processing, and lower costs while maintaining full audit trail.
