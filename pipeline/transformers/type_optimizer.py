"""
Data Type Optimizer
Optimizes DataFrame data types to reduce memory footprint and improve compression
"""
import pandas as pd
from typing import Dict, Any
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class DataTypeOptimizer:
    """Optimize DataFrame data types for better compression"""
    
    def __init__(self, aggressive: bool = False, categorical_threshold: float = 0.5):
        """
        Initialize optimizer
        
        Args:
            aggressive: If True, convert float64 to float32 (may lose precision)
            categorical_threshold: Convert to category if unique ratio < this value
        """
        self.aggressive = aggressive
        self.categorical_threshold = categorical_threshold
    
    def optimize_dtypes(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Optimize DataFrame data types
        
        Args:
            df: Input DataFrame
        
        Returns:
            Tuple of (optimized DataFrame, optimization stats)
        """
        original_size = df.memory_usage(deep=True).sum()
        optimizations = {
            'int_downcast': 0,
            'float_downcast': 0,
            'categorical_conversion': 0,
            'columns_optimized': []
        }
        
        for col in df.columns:
            col_type = df[col].dtype
            original_col_type = str(col_type)
            
            try:
                # Downcast integers
                if col_type in ['int64', 'int32', 'int16']:
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                    new_type = str(df[col].dtype)
                    if new_type != original_col_type:
                        optimizations['int_downcast'] += 1
                        optimizations['columns_optimized'].append({
                            'column': col,
                            'from': original_col_type,
                            'to': new_type
                        })
                        logger.debug(f"  {col}: {original_col_type} → {new_type}")
                
                # Downcast floats (only if aggressive mode)
                elif col_type == 'float64' and self.aggressive:
                    # Check if conversion to float32 is safe
                    max_val = df[col].abs().max()
                    if pd.notna(max_val) and max_val < 3.4e38:  # float32 max
                        df[col] = df[col].astype('float32')
                        optimizations['float_downcast'] += 1
                        optimizations['columns_optimized'].append({
                            'column': col,
                            'from': 'float64',
                            'to': 'float32'
                        })
                        logger.debug(f"  {col}: float64 → float32")
                
                # Convert low-cardinality strings to categorical
                elif col_type == 'object':
                    num_unique = df[col].nunique()
                    num_total = len(df[col])
                    
                    if num_total > 0:
                        unique_ratio = num_unique / num_total
                        
                        if unique_ratio < self.categorical_threshold:
                            df[col] = df[col].astype('category')
                            optimizations['categorical_conversion'] += 1
                            optimizations['columns_optimized'].append({
                                'column': col,
                                'from': 'object',
                                'to': 'category',
                                'unique_ratio': f"{unique_ratio:.2%}"
                            })
                            logger.debug(f"  {col}: object → category (unique ratio: {unique_ratio:.2%})")
            
            except Exception as e:
                logger.warning(f"Failed to optimize column {col}: {e}")
                continue
        
        optimized_size = df.memory_usage(deep=True).sum()
        reduction_bytes = original_size - optimized_size
        reduction_pct = (reduction_bytes / original_size * 100) if original_size > 0 else 0
        
        optimizations['original_size_mb'] = original_size / (1024 * 1024)
        optimizations['optimized_size_mb'] = optimized_size / (1024 * 1024)
        optimizations['reduction_mb'] = reduction_bytes / (1024 * 1024)
        optimizations['reduction_pct'] = reduction_pct
        
        if optimizations['columns_optimized']:
            logger.info(f"Type optimization complete:")
            logger.info(f"  Original size: {optimizations['original_size_mb']:.2f} MB")
            logger.info(f"  Optimized size: {optimizations['optimized_size_mb']:.2f} MB")
            logger.info(f"  Reduction: {optimizations['reduction_mb']:.2f} MB ({reduction_pct:.1f}%)")
            logger.info(f"  Columns optimized: {len(optimizations['columns_optimized'])}")
        else:
            logger.info("Type optimization: No optimizations applied (data types already optimal)")
        
        return df, optimizations


def optimize_dataframe(
    df: pd.DataFrame, 
    aggressive: bool = False,
    categorical_threshold: float = 0.5
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Convenience function to optimize DataFrame data types
    
    Args:
        df: Input DataFrame
        aggressive: If True, convert float64 to float32 (may lose precision)
        categorical_threshold: Convert to category if unique ratio < this value
    
    Returns:
        Tuple of (optimized DataFrame, optimization stats)
    """
    optimizer = DataTypeOptimizer(aggressive=aggressive, categorical_threshold=categorical_threshold)
    return optimizer.optimize_dtypes(df)
