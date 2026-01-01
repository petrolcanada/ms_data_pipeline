# Compression Analysis and Recommendations

## Current Implementation Summary

### What You're Already Doing Well ✅

1. **Parquet Format**: Using columnar Parquet format is excellent for analytics data
   - Inherently compresses better than row-based formats
   - Supports efficient column-level compression
   - Native support for complex data types

2. **Zstd Compression**: Good choice for balanced compression/speed
   - Current setting: `zstd` at level 3
   - Provides ~2-4x compression ratio typically
   - Fast decompression (important for data loading)

3. **Chunked Processing**: Processing 100K rows at a time
   - Prevents memory issues with huge datasets
   - Allows parallel processing opportunities

4. **Encryption After Compression**: Correct order of operations
   - Compress first, then encrypt (encrypted data doesn't compress well)

### Current Compression Stack

```
Raw Data (Snowflake)
    ↓
Pandas DataFrame (in memory)
    ↓
Parquet + Zstd Level 3 (file)
    ↓
AES-256-GCM Encryption (file)
    ↓
Final Encrypted File
```

## Compression Optimization Opportunities

### 1. **Increase Zstd Compression Level** 🔥 HIGH IMPACT

**Current**: Level 3 (fast, moderate compression)
**Recommendation**: Level 9-15 for huge datasets

**Why**: Zstd supports levels 1-22:
- Level 1-3: Fast compression, moderate ratio (~2-3x)
- Level 9-15: Balanced compression, good ratio (~3-5x) ⭐ **RECOMMENDED**
- Level 16-22: Maximum compression, slower (~4-7x)

**Trade-offs**:
- Level 9: ~2x slower compression, ~30-50% better compression
- Level 15: ~5x slower compression, ~50-80% better compression
- Level 19: ~20x slower compression, ~60-100% better compression

**For huge datasets**: The storage/transfer savings outweigh compression time.

**Implementation**:
```python
# In .env file
COMPRESSION_LEVEL=15  # Change from 3 to 15

# Or per-table in tables.yaml
compression:
  algorithm: zstd
  level: 15
```

### 2. **Try Brotli Compression** 🔥 HIGH IMPACT

**Why**: Brotli often achieves better compression than zstd for text-heavy data
- Compression ratio: ~10-20% better than zstd
- Decompression speed: Similar to zstd
- Best for: Financial data with lots of text fields, codes, descriptions

**Implementation**:
```python
# In data_extractor.py
df.to_parquet(
    output_path,
    engine='pyarrow',
    compression='brotli',
    compression_level=11,  # Max level for brotli
    index=False
)
```

**Benchmark Recommendation**: Test both zstd level 15 and brotli level 11 on a sample table.

### 3. **Optimize Data Types Before Compression** 🔥 HIGH IMPACT

**Current**: Storing data as-is from Snowflake
**Recommendation**: Downcast numeric types and use categorical encoding

**Why**: Smaller data types = better compression
- int64 → int32 or int16 (if values fit): 50-75% size reduction
- float64 → float32 (if precision allows): 50% size reduction
- object → category (for low-cardinality strings): 80-95% size reduction

**Example Savings**:
```
Original: 1 million rows × 10 columns × 8 bytes = 80 MB
Optimized: 1 million rows × 10 columns × 4 bytes = 40 MB
After compression: 40 MB → ~8-10 MB (vs 80 MB → ~20-25 MB)
```

**Implementation**:
```python
def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame data types for compression"""
    for col in df.columns:
        col_type = df[col].dtype
        
        # Downcast integers
        if col_type in ['int64', 'int32']:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        # Downcast floats (be careful with precision!)
        elif col_type == 'float64':
            # Only if precision loss is acceptable
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Convert low-cardinality strings to categorical
        elif col_type == 'object':
            num_unique = df[col].nunique()
            num_total = len(df[col])
            if num_unique / num_total < 0.5:  # Less than 50% unique
                df[col] = df[col].astype('category')
    
    return df
```

### 4. **Enable Parquet Encoding Options** 🔥 MEDIUM IMPACT

**Current**: Using PyArrow defaults
**Recommendation**: Explicitly enable advanced encodings

**Why**: Parquet supports multiple encoding schemes:
- **Dictionary encoding**: For repeated values (IDs, codes, categories)
- **Delta encoding**: For timestamps, sequential IDs
- **Bit-packing**: For small integers
- **RLE (Run-Length Encoding)**: For repeated values

**Implementation**:
```python
import pyarrow as pa
import pyarrow.parquet as pq

# Create Parquet writer with custom options
df.to_parquet(
    output_path,
    engine='pyarrow',
    compression='zstd',
    compression_level=15,
    use_dictionary=True,  # Enable dictionary encoding
    use_byte_stream_split=True,  # Better for floating point
    data_page_size=1024*1024,  # 1MB pages (larger = better compression)
    index=False
)
```

### 5. **Multi-Level Compression** 🔥 LOW-MEDIUM IMPACT

**Current**: Single-level compression (Parquet only)
**Recommendation**: Add optional archive-level compression

**Why**: For transferring multiple files, tar.zst can provide additional 10-20% savings

**Use Case**: When you have many small Parquet files per table

**Implementation**:
```python
import tarfile

# After creating all Parquet files
with tarfile.open(f'{table_name}.tar.zst', 'w:zst') as tar:
    for parquet_file in parquet_files:
        tar.add(parquet_file)
```

**Note**: Only beneficial if you have many small files. For large single files, skip this.

### 6. **Adaptive Compression Selection** 🔥 MEDIUM IMPACT

**Current**: Fixed compression settings for all tables
**Recommendation**: Auto-select compression based on data characteristics

**Why**: Different data types compress differently
- Numeric-heavy data: zstd performs well
- Text-heavy data: brotli or gzip performs better
- Mixed data: Test and choose

**Implementation**:
```python
def select_compression(df: pd.DataFrame) -> tuple[str, int]:
    """Auto-select compression based on data characteristics"""
    
    # Analyze data types
    numeric_cols = df.select_dtypes(include=['number']).columns
    text_cols = df.select_dtypes(include=['object']).columns
    
    numeric_ratio = len(numeric_cols) / len(df.columns)
    
    # Test compression on sample
    sample = df.head(10000)
    
    if numeric_ratio > 0.7:
        # Mostly numeric - use zstd
        return 'zstd', 15
    elif numeric_ratio < 0.3:
        # Mostly text - use brotli
        return 'brotli', 11
    else:
        # Mixed - benchmark both
        return 'zstd', 15  # Default to zstd
```

## Recommended Implementation Priority

### Phase 1: Quick Wins (Immediate) ⚡
1. **Increase zstd level to 15** - Change one config value
   - Expected savings: 30-50% additional compression
   - Implementation time: 5 minutes
   - Risk: Low (just slower compression)

2. **Add data type optimization** - Add preprocessing step
   - Expected savings: 20-40% additional compression
   - Implementation time: 1-2 hours
   - Risk: Medium (test for precision loss)

### Phase 2: Advanced Optimization (1-2 days) 🚀
3. **Benchmark brotli vs zstd** - Compare algorithms
   - Expected savings: 10-20% additional compression
   - Implementation time: 2-4 hours
   - Risk: Low (just testing)

4. **Enable advanced Parquet encodings** - Optimize Parquet settings
   - Expected savings: 10-15% additional compression
   - Implementation time: 2-3 hours
   - Risk: Low (PyArrow handles it)

### Phase 3: Automation (2-3 days) 🤖
5. **Implement adaptive compression** - Auto-select best algorithm
   - Expected savings: Varies by table
   - Implementation time: 1-2 days
   - Risk: Medium (needs testing)

6. **Add compression benchmarking** - Monitor and report
   - Expected savings: Enables data-driven decisions
   - Implementation time: 1 day
   - Risk: Low (monitoring only)

## Expected Overall Compression Improvements

### Conservative Estimate
- Current: ~3x compression (zstd level 3)
- With optimizations: ~6-8x compression
- **Storage savings: 50-60%**

### Aggressive Estimate
- Current: ~3x compression
- With all optimizations: ~10-15x compression
- **Storage savings: 70-80%**

### Real-World Example
```
Original Snowflake table: 10 GB
Current compression (zstd-3): ~3.3 GB
Optimized compression: ~1.5-2 GB

Savings: 1.3-1.8 GB per table
For 100 tables: 130-180 GB saved
```

## Compression Algorithm Comparison

| Algorithm | Level | Ratio | Comp Speed | Decomp Speed | Best For |
|-----------|-------|-------|------------|--------------|----------|
| snappy    | N/A   | 2-3x  | Very Fast  | Very Fast    | Real-time processing |
| lz4       | N/A   | 2-3x  | Very Fast  | Very Fast    | Real-time processing |
| zstd      | 3     | 3-4x  | Fast       | Very Fast    | Balanced (current) |
| zstd      | 9     | 4-5x  | Medium     | Very Fast    | **Recommended** |
| zstd      | 15    | 5-6x  | Slow       | Very Fast    | **Huge datasets** |
| zstd      | 19    | 6-7x  | Very Slow  | Very Fast    | Archive/cold storage |
| gzip      | 6     | 4-5x  | Medium     | Fast         | Text-heavy data |
| gzip      | 9     | 5-6x  | Slow       | Fast         | Maximum compatibility |
| brotli    | 9     | 5-6x  | Slow       | Fast         | Text-heavy data |
| brotli    | 11    | 6-7x  | Very Slow  | Fast         | **Text-heavy huge datasets** |

## Testing Recommendations

### Benchmark Script
```python
import time
import pandas as pd
from pathlib import Path

def benchmark_compression(df: pd.DataFrame, algorithms: list):
    """Benchmark different compression algorithms"""
    results = []
    
    for algo, level in algorithms:
        start = time.time()
        
        # Compress
        output = Path(f'test_{algo}_{level}.parquet')
        df.to_parquet(
            output,
            compression=algo,
            compression_level=level if algo in ['zstd', 'gzip', 'brotli'] else None
        )
        
        comp_time = time.time() - start
        comp_size = output.stat().st_size
        
        # Decompress
        start = time.time()
        df_read = pd.read_parquet(output)
        decomp_time = time.time() - start
        
        results.append({
            'algorithm': algo,
            'level': level,
            'size_mb': comp_size / (1024*1024),
            'compression_time': comp_time,
            'decompression_time': decomp_time,
            'compression_ratio': len(df) * df.memory_usage(deep=True).sum() / comp_size
        })
        
        output.unlink()
    
    return pd.DataFrame(results)

# Test on your data
algorithms = [
    ('zstd', 3),   # Current
    ('zstd', 9),   # Recommended
    ('zstd', 15),  # Huge datasets
    ('brotli', 9),
    ('brotli', 11),
    ('gzip', 9)
]

results = benchmark_compression(your_df, algorithms)
print(results.to_string())
```

## Configuration Examples

### Option 1: Global Settings (.env)
```bash
# Conservative (faster, good compression)
COMPRESSION_TYPE=zstd
COMPRESSION_LEVEL=9

# Aggressive (slower, best compression)
COMPRESSION_TYPE=zstd
COMPRESSION_LEVEL=15

# Text-heavy data
COMPRESSION_TYPE=brotli
COMPRESSION_LEVEL=11
```

### Option 2: Per-Table Settings (tables.yaml)
```yaml
tables:
  - name: "financial_transactions"
    compression:
      algorithm: zstd
      level: 15
      optimize_types: true
    
  - name: "customer_descriptions"
    compression:
      algorithm: brotli
      level: 11
      optimize_types: true
    
  - name: "real_time_quotes"
    compression:
      algorithm: snappy  # Fast for real-time
      optimize_types: false
```

## Next Steps

1. **Review requirements document** - Ensure all requirements align with your needs
2. **Run benchmark test** - Test compression algorithms on sample data
3. **Implement Phase 1 optimizations** - Quick wins (zstd level 15 + type optimization)
4. **Measure results** - Compare before/after compression ratios
5. **Proceed to Phase 2** - If results are good, implement advanced features

Would you like me to proceed with creating the design document for these compression optimizations?
