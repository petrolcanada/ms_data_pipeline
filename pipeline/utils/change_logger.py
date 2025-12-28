"""
Change Logger
Logs metadata changes to file for audit trail
"""
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class ChangeLogger:
    """
    Log metadata changes to file
    
    Creates and maintains change log files for each table
    """
    
    def __init__(self, changes_dir: str = "metadata/changes"):
        """
        Initialize change logger
        
        Args:
            changes_dir: Directory to store change logs
        """
        self.changes_dir = Path(changes_dir)
        self.changes_dir.mkdir(parents=True, exist_ok=True)
    
    def log_change(self, table_name: str, changes: List[Dict], summary: str):
        """
        Log a metadata change
        
        Args:
            table_name: Name of the table
            changes: List of change dictionaries
            summary: Human-readable summary
        """
        log_file = self.changes_dir / f"{table_name}_changes.log"
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Format log entry
        log_entry = f"\n{timestamp} - Schema Change Detected\n"
        log_entry += f"Summary: {summary}\n"
        
        for change in changes:
            change_type = change['type']
            
            if change_type == 'column_added':
                col = change['column']
                details = change['details']
                data_type = details.get('data_type', 'UNKNOWN')
                nullable = "NULL" if details.get('nullable') else "NOT NULL"
                log_entry += f"  • Column added: {col} ({data_type}, {nullable})\n"
            
            elif change_type == 'column_removed':
                col = change['column']
                log_entry += f"  • Column removed: {col}\n"
            
            elif change_type == 'column_type_changed':
                col = change['column']
                details = change['details']
                old_type = details.get('old_type', 'UNKNOWN')
                new_type = details.get('new_type', 'UNKNOWN')
                log_entry += f"  • Column type changed: {col} ({old_type} → {new_type})\n"
            
            elif change_type == 'column_nullable_changed':
                col = change['column']
                details = change['details']
                old_null = "NULL" if details.get('old_nullable') else "NOT NULL"
                new_null = "NULL" if details.get('new_nullable') else "NOT NULL"
                log_entry += f"  • Column nullable changed: {col} ({old_null} → {new_null})\n"
            
            elif change_type == 'primary_key_changed':
                details = change['details']
                old_pk = ', '.join(details.get('old_pk', []))
                new_pk = ', '.join(details.get('new_pk', []))
                log_entry += f"  • Primary key changed: ({old_pk}) → ({new_pk})\n"
            
            else:
                log_entry += f"  • {change_type}\n"
        
        log_entry += f"  Archived: {table_name}_{datetime.now().strftime('%Y%m%d')}\n"
        
        # Append to log file
        with open(log_file, 'a') as f:
            f.write(log_entry)
        
        logger.info(f"Change logged to: {log_file}")
    
    def log_initial_extraction(self, table_name: str):
        """
        Log initial metadata extraction
        
        Args:
            table_name: Name of the table
        """
        log_file = self.changes_dir / f"{table_name}_changes.log"
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        log_entry = f"{timestamp} - Initial Metadata Extraction\n"
        log_entry += f"  Created: {table_name}_metadata.json\n"
        log_entry += f"  Created: {table_name}_create.sql\n\n"
        
        with open(log_file, 'w') as f:
            f.write(log_entry)
        
        logger.info(f"Initial extraction logged to: {log_file}")
    
    def get_change_history(self, table_name: str) -> str:
        """
        Get change history for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Change history as string
        """
        log_file = self.changes_dir / f"{table_name}_changes.log"
        
        if not log_file.exists():
            return f"No change history found for {table_name}"
        
        with open(log_file, 'r') as f:
            return f.read()
    
    def format_change_log(self, table_name: str, max_entries: int = 10) -> str:
        """
        Format change log for display
        
        Args:
            table_name: Name of the table
            max_entries: Maximum number of entries to show
            
        Returns:
            Formatted change log
        """
        history = self.get_change_history(table_name)
        
        if history.startswith("No change history"):
            return history
        
        # Split into entries
        entries = history.split('\n\n')
        
        # Take last N entries
        recent_entries = entries[-max_entries:] if len(entries) > max_entries else entries
        
        formatted = f"Change History for {table_name} (last {len(recent_entries)} entries):\n"
        formatted += "=" * 70 + "\n"
        formatted += '\n\n'.join(recent_entries)
        
        return formatted
