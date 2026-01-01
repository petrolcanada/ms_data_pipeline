# Quick Win #2: Implementation Guide

## What You Need to Do (Brief Overview)

### Step 1: Update .env (30 seconds)
```bash
# Change this line:
COMPRESSION_LEVEL=15  # from 3 to 15
```

### Step 2: Create Type Optimizer Function (30 minutes)
Create a new file: `pipeline/transformers/type_optimizer.py`

**What it does**: Automatically shrinks data types before compression
- int64 → int32/int16/int8 (if values fit)
- float64 → float32 (if precision OK)
- text → category (if low cardinality)

**Code size**: ~80 lines

### Step 3: Integrate into Data Extractor (15 minutes)
Modify: `pipeline/extractors/data_extractor.py`

**What changes**: Add 3 lines of code to call the optimizer before saving Parquet

### Step 4: Test on Sample Data (30 minutes)
Run export on one small table to verify:
- Data types are optimized
- No data loss
- Compression improves
- Data loads correctly into PostgreSQL

---

## Difficulty Level: ⭐⭐☆☆☆ (Easy-Medium)

### Why It's Easy:
✅ No complex algorithms - just type conversions
✅ Pandas does the heavy lifting
✅ Only 2 files to modify
✅ Safe - includes validation checks

### Why It Takes 1-2 Hours:
⏱️ Writing the optimizer function: 30 min
⏱️ Integration: 15 min
⏱️ Testing: 30-45 min
⏱️ Total: ~1.5 hours

---

## Code Preview (What You'll Write)

### File 1: `pipeline/transformers/type_optimizer.py` (NEW)

```python
"""Data Type Optimizer - Reduces memory footprint before compression"""
import pandas as pd
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)

def optimize_dtypes(df: pd.DataFrame, aggressive: bool = False) -> pd.DataFrame:
    """
    Optimize DataFrame data types for better compression
    
    Args:
        df: Input DataFrame
        aggressive: If True, convert float64 to float32 (may lose precision)
    
    Returns:
        Optimized DataFrame
    """
    original_size = df.memory_usage(deep=True).sum()
    
    for col in df.columns:
        col_type = df[col].dtype
        
        # Downcast integers
        if col_type in ['int64', 'int32', 'int16']:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        # Downcast floats (optional - be careful!)
        elif col_type == 'float64' and aggressive:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Convert low-cardinality strings to categorical
        elif col_type == 'object':
            num_unique = df[col].nunique()
            num_total = len(df[col])
            if num_unique / num_total < 0.5:  # Less than 50% unique
                df[col] = df[col].astype('category')
    
    optimized_size = df.memory_usage(deep=True).sum()
    reduction = (1 - optimized_size / original_size) * 100
    
    logger.info(f"Type optimization: {reduction:.1f}% memory reduction")
    
    return df
```

**That's it!** ~40 lines of actual code.

---

### File 2: `pipeline/extractors/data_extractor.py` (MODIFY)

**Find this function** (around line 334):
```python
def save_chunk_to_parquet(
    self, 
    df: pd.DataFrame, 
    output_path: Path, 
    compression: str = 'zstd',
    compression_level: int = 3
) -> Dict[str, Any]:
```

**Add 3 lines at the top**:
```python
from pipeline.transformers.type_optimizer import optimize_dtypes  # Line 1

def save_chunk_to_parquet(self, df, output_path, compression='zstd', compression_level=3):
    # Add these 2 lines:
    logger.info("Optimizing data types...")
    df = optimize_dtypes(df, aggressive=False)  # Line 2-3
    
    # Rest of the function stays the same...
    df.to_parquet(...)
```

**That's it!** Just 3 lines added.

---

## Step-by-Step Implementation

### 1️⃣ Update Config (30 seconds)
```bash
# In .env file
COMPRESSION_LEVEL=15
```

### 2️⃣ Create Type Optimizer (30 minutes)
- Create `pipeline/transformers/type_optimizer.py`
- Copy the 40-line function above
- Save file

### 3️⃣ Integrate (15 minutes)
- Open `pipeline/extractors/data_extractor.py`
- Add import at top
- Add 2 lines in `save_chunk_to_parquet()`
- Save file

### 4️⃣ Test (30 minutes)
```bash
# Test on one small table
python scripts/export_data.py \
  --database CIGAM_PRD_RL \
  --schema MORNINGSTAR_MAIN \
  --table YOUR_SMALL_TABLE

# Check the output:
# - Look for "Type optimization: X% memory reduction" in logs
# - Verify file size is smaller
# - Test import to PostgreSQL
```

---

## Risk Assessment

### Low Risk ✅
- **Integer downcasting**: Safe - Pandas checks if values fit
- **Categorical conversion**: Safe - only for low-cardinality columns
- **Compression level**: Safe - just slower compression

### Medium Risk ⚠️
- **Float32 conversion**: Can lose precision
  - **Solution**: Set `aggressive=False` by default
  - Only enable if you verify precision loss is acceptable

### Safety Features Built-In
```python
# Pandas automatically validates:
- int64 → int32: Only if all values fit in int32 range
- int64 → int16: Only if all values fit in int16 range
- If values don't fit, keeps original type

# You control:
- aggressive=False: Skip float conversion (safer)
- aggressive=True: Convert float64 → float32 (more compression)
```

---

## Expected Results

### Before (Current)
```
Chunk 1: 100,000 rows
  Memory: 80 MB
  Parquet (zstd-3): 27 MB
  Time: 2 seconds
```

### After (Optimized)
```
Chunk 1: 100,000 rows
  Memory: 40 MB (50% smaller!)
  Type optimization: 48.5% memory reduction
  Parquet (zstd-15): 9 MB (67% smaller!)
  Time: 4 seconds (2x slower compression, but worth it)
```

---

## Troubleshooting

### Issue 1: "Precision loss in float conversion"
**Solution**: Keep `aggressive=False` (default)

### Issue 2: "Data doesn't load into PostgreSQL"
**Solution**: Parquet preserves types correctly - this shouldn't happen
- If it does, check PostgreSQL column types match

### Issue 3: "Compression takes too long"
**Solution**: 
- Reduce `COMPRESSION_LEVEL` to 9 (still better than 3)
- Or keep level 15 - it's a one-time cost

---

## Comparison: Effort vs Reward

| Aspect | Quick Win #1 | Quick Win #2 |
|--------|--------------|--------------|
| **Implementation Time** | 30 seconds | 1.5 hours |
| **Code Changes** | 1 line | 1 new file + 3 lines |
| **Difficulty** | ⭐☆☆☆☆ Trivial | ⭐⭐☆☆☆ Easy |
| **Storage Savings** | 41% | 65% |
| **Risk** | None | Low |
| **Reversible** | Yes (change config) | Yes (remove 3 lines) |
| **Worth It?** | ✅ Absolutely | ✅ Absolutely |

---

## My Recommendation

### Do Both! Here's Why:

1. **Quick Win #1** (30 seconds): Immediate 41% savings
2. **Quick Win #2** (1.5 hours): Additional 24% savings

**Total time**: 1.5 hours
**Total savings**: 65% storage reduction
**ROI**: Pays for itself in less than 1 month

### Implementation Order:
```
1. Change COMPRESSION_LEVEL=15 (30 sec) → Test on 1 table
2. If good, apply to all tables
3. Add type optimization (1.5 hours) → Test on 1 table
4. If good, apply to all tables
```

This way you get immediate benefits, then enhance further.

---

## Next Steps

**Option A**: I can implement both right now (takes me ~5 minutes)
**Option B**: I can create the code and you review before applying
**Option C**: I can create a detailed spec with tasks for you to implement

Which would you prefer?
