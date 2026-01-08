"""
Configuration Validator
Validates table configuration including index columns against table metadata
"""
from typing import Dict, Any, List
from dataclasses import dataclass
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class ConfigurationError(Exception):
    """Raised when table configuration is invalid"""
    pass


class IndexValidationError(ConfigurationError):
    """Raised when index configuration fails validation"""
    pass


@dataclass
class ValidationResult:
    """Result of configuration validation"""
    success: bool
    errors: List[str]
    warnings: List[str]
    
    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


def validate_index_configuration(
    table_name: str,
    index_columns: List[str],
    table_metadata: Dict[str, Any]
) -> ValidationResult:
    """
    Validate that all configured index columns exist in table metadata.
    
    Args:
        table_name: Name of the table being validated
        index_columns: List of column names to index
        table_metadata: Metadata extracted from Snowflake
        
    Returns:
        ValidationResult with success status and error/warning messages
        
    Raises:
        IndexValidationError: If validation fails
    """
    errors = []
    warnings = []
    
    # Handle empty index list - this is valid
    if not index_columns:
        logger.debug(f"No indexes configured for table {table_name}")
        return ValidationResult(success=True, errors=[], warnings=[])
    
    # Get available columns from metadata
    available_columns = {col["name"] for col in table_metadata["columns"]}
    
    # Check for duplicate columns in index list
    seen_columns = set()
    for col in index_columns:
        if col in seen_columns:
            warnings.append(f"Duplicate column '{col}' in index list for table '{table_name}', will create only once")
        seen_columns.add(col)
    
    # Validate each index column exists in table
    invalid_columns = []
    for col in seen_columns:  # Use seen_columns to avoid duplicate checks
        if col not in available_columns:
            invalid_columns.append(col)
    
    if invalid_columns:
        available_list = sorted(list(available_columns))
        error_msg = (
            f"Index column(s) {invalid_columns} not found in table '{table_name}'. "
            f"Available columns: {available_list}"
        )
        errors.append(error_msg)
        
        # Raise exception for invalid columns
        raise IndexValidationError(error_msg)
    
    # Log success
    if warnings:
        for warning in warnings:
            logger.warning(warning)
    
    logger.info(f"✅ Validated indexes for table {table_name}: {list(seen_columns)}")
    
    return ValidationResult(success=True, errors=errors, warnings=warnings)
