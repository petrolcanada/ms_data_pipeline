# Compression Comparison Guide

## Overview

The `compare_compression.py` script allows you to test different compression configurations side-by-side on your actual data to see real-world differences.

---

## Quick Start

### Test All Configurations (Recommended):

```bash
python scripts/compare_compression.py --table YOUR_TABLE_NAME
```

This will test:
1. **Baseline** - zstd level 3, no type optimization (current default)
2. **Quick Win #1** - zstd level 15, no type optimization
3. **Type Optimization Only** - zstd level 3 with type optimization
4. **Quick Win #2** - zstd level 15 with type optimization (recommended)

---

## Usage Examples

### 1. Test Specific Table:
```bash
python scripts/compare_compression.py --table financial_data
```

### 2. Test with Custom Chunk Size:
```bash
python scripts/compare_compression.py --table financial_data --chunk-size 50000
```

### 3. Test Only Specific Configurations:
```bash
# Test only baseline vs Quick Win #2
python scripts/compare_compression.py --table financial_data --configs baseline both

# Test only Quick Win #1 vs Quick Win #2
python scripts/compare_compression.py --table financial_data --configs level15 both
```

### 4. Use Password File:
```bash
python scripts/compare_compression.py --table financial_data --password-file ~/.encryption_key
```

---

## Configuration Options

### Available Configs:

| Config Name | Compression | Level | Type Optimization | Description |
|-------------|-------------|-------|-------------------|-------------|
| `baseline` | zstd | 3 | No | Current default |
| `level15` | zstd | 15 | No | Quick Win #1 only |
| `types` | zstd | 3 | Yes | Type optimization only |
| `both` | zstd | 15 | Yes | Quick Win #2 (recommended) |
| `all` | - | - | - | Test all above (default) |

### Command Line Options:

```bash
--table TABLE_NAME          # Required: Table to test
--chunk-size SIZE           # Optional: Rows per chunk (default: 100000)
--configs CONFIG [CONFIG]   # Optional: Which configs to test (default: all)
--password-file PATH        # Optional: Path to encryption password file
```

---

## What It Does

### For Each Configuration:

1. **Extracts data** from Snowflake (same data for all tests)
2. **Applies compression** with specified settings
3. **Encrypts files** (same encryption for all)
4. **Measures**:
   - Compressed file size
   - Encrypted file size
   - Compression time
   - Encryption time
   - Type optimization reduction (if enabled)

### Output:

```
==================================================================================================
COMPRESSION COMPARISON RESULTS
==================================================================================================

Configuration             Size (MB)    Savings      Comp Time    Total Time   Type Opt
----------------------------------------------------------------------------------------------------
Baseline (zstd-3)         27.45        0.0%         2.34s        5.67s        N/A
Quick Win #1 (zstd-15)    16.23        40.9%        7.12s        10.45s       N/A
Type Optimization Only    14.56        46.9%        2.89s        6.12s        48.5%
Quick Win #2 (zstd-15+)   9.12         66.8%        6.78s        10.01s       48.5%

==================================================================================================
SUMMARY
==================================================================================================

🏆 Best Compression: Quick Win #2 (zstd-15 + types)
   Size: 9.12 MB
   Savings vs baseline: 66.8%

⚡ Fastest: Baseline (zstd-3)
   Time: 5.67s

💡 Recommendation:
   'Quick Win #2 (zstd-15 + types)' saves 66.8% more space
   but takes 4.34s longer (76.5% slower)
   For huge datasets, the space savings are usually worth the extra time.
```

---

## Understanding the Results

### Size (MB):
- Final encrypted file size
- **Lower is better**
- This is what you'll store and transfer

### Savings:
- Percentage reduction compared to baseline
- **Higher is better**
- Shows how much space you save

### Comp Time:
- Time spent compressing data
- Does NOT include extraction or encryption
- **Lower is better** (but space savings often more important)

### Total Time:
- Complete time including extraction, compression, and encryption
- **Lower is better**
- For huge datasets, extra time is usually worth the space savings

### Type Opt:
- Average memory reduction from type optimization
- Only shown when type optimization is enabled
- Indicates how much data was reduced before compression

---

## Interpreting Results

### Scenario 1: Similar File Sizes

If all configurations produce similar file sizes:

**Possible Reasons:**
1. **Data is already highly compressed** - Your data might already be in an optimal format
2. **Small sample size** - Test with more chunks/rows
3. **Data characteristics** - Some data types don't compress well (e.g., already compressed images, random data)

**What to Check:**
```bash
# Test with more data
python scripts/compare_compression.py --table YOUR_TABLE --chunk-size 500000

# Check the data types in your table
# Look for: lots of integers, floats, repeated strings
```

---

### Scenario 2: Type Optimization Shows No Improvement

If "Type Optimization Only" is similar to "Baseline":

**Possible Reasons:**
1. **Data types already optimal** - Your data might already use int32, int16, categories
2. **High cardinality strings** - Strings with many unique values don't benefit from categorical conversion
3. **Already compressed data** - Data extracted from Snowflake might already be optimized

**What to Check:**
- Look at the logs for "Type optimization: X% memory reduction"
- If it shows 0% or very low, your data types are already optimal

---

### Scenario 3: Compression Level Shows No Improvement

If "Quick Win #1 (zstd-15)" is similar to "Baseline (zstd-3)":

**Possible Reasons:**
1. **Data is incompressible** - Random or already compressed data
2. **Parquet already compressing well** - Columnar format + zstd-3 might be enough
3. **Small data size** - Compression improvements more visible on larger datasets

**What to Try:**
```bash
# Test with brotli instead
# (You'll need to modify the script or test manually)
```

---

## Real-World Example

### Test on 1 Million Rows:

```bash
python scripts/compare_compression.py --table large_financial_table --chunk-size 100000
```

**Expected Results:**

```
Configuration             Size (MB)    Savings      Comp Time    Total Time   Type Opt
----------------------------------------------------------------------------------------------------
Baseline (zstd-3)         274.50       0.0%         23.40s       56.70s       N/A
Quick Win #1 (zstd-15)    162.30       40.9%        71.20s       104.50s      N/A
Type Optimization Only    145.60       46.9%        28.90s       61.20s       48.5%
Quick Win #2 (zstd-15+)   91.20        66.8%        67.80s       100.10s      48.5%
```

**Analysis:**
- Quick Win #2 saves **183 MB** (66.8%) compared to baseline
- Takes **43 seconds longer** (76% slower)
- For a 10 GB table, this would save **6.7 GB** of storage
- At $0.10/GB/month, saves **$0.67/month per table**
- For 100 tables: **$67/month = $804/year savings**

---

## After Testing

### 1. Review Results:
```bash
# Check the comparison results
cat exports/YOUR_TABLE_comparison/comparison_results.json
```

### 2. Choose Best Configuration:

Based on your priorities:

**Priority: Maximum Space Savings**
→ Use Quick Win #2 (zstd-15 + types)

**Priority: Speed**
→ Use Baseline or Type Optimization Only

**Priority: Balanced**
→ Use Quick Win #1 (zstd-15 only)

### 3. Update Your .env:

```bash
# For maximum savings (recommended for huge datasets)
COMPRESSION_LEVEL=15

# For speed
COMPRESSION_LEVEL=3

# For balanced
COMPRESSION_LEVEL=9
```

### 4. Clean Up Test Files:

```bash
# Delete the comparison folder
rm -rf exports/YOUR_TABLE_comparison/
```

---

## Troubleshooting

### Issue: "File sizes are all the same"

**Check:**
1. Are you testing on enough data? Try larger chunk-size
2. Look at the logs - is type optimization actually running?
3. Check your data types - maybe they're already optimal

**Solution:**
```bash
# Test with more data
python scripts/compare_compression.py --table YOUR_TABLE --chunk-size 500000
```

---

### Issue: "Script is too slow"

**Solution:**
```bash
# Test only baseline vs Quick Win #2
python scripts/compare_compression.py --table YOUR_TABLE --configs baseline both

# Or use smaller chunk size
python scripts/compare_compression.py --table YOUR_TABLE --chunk-size 50000
```

---

### Issue: "Out of memory"

**Solution:**
```bash
# Reduce chunk size
python scripts/compare_compression.py --table YOUR_TABLE --chunk-size 50000
```

---

## Tips for Best Results

### 1. Test on Representative Data:
- Choose a table that's typical of your dataset
- Use enough rows to see meaningful differences (100K+ recommended)

### 2. Test Multiple Tables:
- Different tables may benefit differently
- Text-heavy tables: Try brotli
- Numeric-heavy tables: zstd works well

### 3. Consider Your Use Case:
- **One-time export**: Speed matters less, use maximum compression
- **Frequent exports**: Balance speed and compression
- **Real-time**: Use snappy or lz4 (fast compression)

### 4. Monitor Actual Usage:
- After choosing a configuration, monitor:
  - Storage costs
  - Transfer times
  - Compression times
- Adjust if needed

---

## Next Steps

1. **Run the comparison** on your actual data
2. **Review the results** and choose the best configuration
3. **Update .env** with your chosen settings
4. **Run full export** with new settings
5. **Monitor and adjust** as needed

---

## Questions?

If results are unexpected:
1. Check the detailed logs in the console
2. Review `comparison_results.json` for detailed stats
3. Try testing with different chunk sizes
4. Consider your data characteristics (types, cardinality, etc.)

**Remember:** The goal is to find the best balance between compression ratio and speed for YOUR specific data!
