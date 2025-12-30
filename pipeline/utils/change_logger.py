"""
Change Logger
Logs metadata changes to console for immediate visibility
Versioned files are kept in metadata/schemas and metadata/ddl folders
"""
from datetime import datetime
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class ChangeLogger:
    """
    Log metadata changes to console
    
    Provides immediate visibility of schema changes without separate log files
    """
    
    def log_change(self, table_name: str, changes: list, summary: str):
        """
        Log a metadata change to console
        
        Args:
            table_name: Name of the table
            changes: List of change dictionaries
            summary: Human-readable summary
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        logger.warning(f"[{timestamp}] Schema change detected for {table_name}")
        logger.warning(f"Summary: {summary}")
        
        for change in changes:
            change_type = change['type']
            
            if change_type == 'column_added':
                col = change['column']
                details = change['details']
                data_type = details.get('data_type', 'UNKNOWN')
                nullable = "NULL" if details.get('nullable') else "NOT NULL"
                logger.warning(f"  • Column added: {col} ({data_type}, {nullable})")
            
            elif change_type == 'column_removed':
                col = change['column']
                logger.warning(f"  • Column removed: {col}")
            
            elif change_type == 'column_type_changed':
                col = change['column']
                details = change['details']
                old_type = details.get('old_type', 'UNKNOWN')
                new_type = details.get('new_type', 'UNKNOWN')
                logger.warning(f"  • Column type changed: {col} ({old_type} → {new_type})")
            
            elif change_type == 'column_nullable_changed':
                col = change['column']
                details = change['details']
                old_null = "NULL" if details.get('old_nullable') else "NOT NULL"
                new_null = "NULL" if details.get('new_nullable') else "NOT NULL"
                logger.warning(f"  • Column nullable changed: {col} ({old_null} → {new_null})")
            
            elif change_type == 'primary_key_changed':
                details = change['details']
                old_pk = ', '.join(details.get('old_pk', []))
                new_pk = ', '.join(details.get('new_pk', []))
                logger.warning(f"  • Primary key changed: ({old_pk}) → ({new_pk})")
        
        logger.warning(f"Archived: {table_name}_{datetime.now().strftime('%Y%m%d')}_metadata.json")
        logger.warning(f"Archived: {table_name}_{datetime.now().strftime('%Y%m%d')}_create.sql")
    
    def log_initial_extraction(self, table_name: str):
        """
        Log initial metadata extraction to console
        
        Args:
            table_name: Name of the table
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"[{timestamp}] Initial metadata extraction for {table_name}")
        logger.info(f"  Created: {table_name}_metadata.json")
        logger.info(f"  Created: {table_name}_create.sql")
