"""
Change Logger
Logs metadata changes to console and persistent log files
Versioned files are kept in metadata/schemas and metadata/ddl folders
"""
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class ChangeLogger:
    """
    Log metadata changes to console and persistent files
    
    Provides immediate visibility via console and permanent audit trail via log files
    Supports encryption when obfuscation is enabled
    """
    
    def __init__(self, log_dir: Path = None, obfuscator=None):
        """
        Initialize ChangeLogger
        
        Args:
            log_dir: Directory for change log files (default: metadata/changes)
            obfuscator: Optional MetadataObfuscator instance for encryption
        """
        self.log_dir = log_dir or Path("metadata/changes")
        self.obfuscator = obfuscator
        self._ensure_log_directory()
    
    def _ensure_log_directory(self):
        """Create log directory if it doesn't exist"""
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Change log directory ensured: {self.log_dir}")
        except Exception as e:
            logger.error(f"Failed to create change log directory {self.log_dir}: {e}")
            # Non-fatal - console logging will still work
    
    def log_change(
        self,
        table_name: str,
        changes: List[Dict],
        summary: str,
        archived_files: Optional[Dict[str, Path]] = None,
        password: Optional[str] = None
    ):
        """
        Log a metadata change to console and persistent file
        
        Args:
            table_name: Name of the table
            changes: List of change dictionaries
            summary: Human-readable summary
            archived_files: Dict with 'metadata' and 'ddl' archived file paths
            password: Encryption password (required if obfuscation enabled)
        """
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Log to console (existing behavior)
        logger.warning(f"[{timestamp}] Schema change detected for {table_name}")
        logger.warning(f"Summary: {summary}")
        
        for change in changes:
            self._log_change_to_console(change)
        
        # Log archived files to console
        if archived_files:
            if archived_files.get('metadata'):
                logger.warning(f"Archived: {archived_files['metadata']}")
            if archived_files.get('ddl'):
                logger.warning(f"Archived: {archived_files['ddl']}")
        else:
            # Fallback to old format if archived_files not provided
            logger.warning(f"Archived: {table_name}_{datetime.now().strftime('%Y%m%d')}_metadata.json")
            logger.warning(f"Archived: {table_name}_{datetime.now().strftime('%Y%m%d')}_create.sql")
        
        # Write to persistent log file
        self._write_to_log_file(table_name, timestamp, summary, changes, archived_files, password)
    
    def _log_change_to_console(self, change: Dict):
        """Log a single change to console"""
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
        
        elif change_type == 'column_position_changed':
            col = change['column']
            details = change['details']
            old_pos = details.get('old_position')
            new_pos = details.get('new_position')
            logger.warning(f"  • Column position changed: {col} (position {old_pos} → {new_pos})")
        
        elif change_type == 'primary_key_changed':
            details = change['details']
            old_pk = ', '.join(details.get('old_pk', []))
            new_pk = ', '.join(details.get('new_pk', []))
            logger.warning(f"  • Primary key changed: ({old_pk}) → ({new_pk})")
        
        elif change_type == 'foreign_keys_changed':
            details = change['details']
            old_count = details.get('old_count', 0)
            new_count = details.get('new_count', 0)
            logger.warning(f"  • Foreign keys changed: {old_count} → {new_count}")
        
        elif change_type == 'comment_changed':
            logger.warning(f"  • Table comment changed")
        
        elif change_type == 'clustering_key_changed':
            logger.warning(f"  • Clustering key changed")
        
        else:
            logger.warning(f"  • {change_type}: {change.get('column', 'N/A')}")
    
    def _write_to_log_file(
        self,
        table_name: str,
        timestamp: str,
        summary: str,
        changes: List[Dict],
        archived_files: Optional[Dict[str, Path]] = None,
        password: Optional[str] = None
    ):
        """Write change entry to persistent log file (encrypted if obfuscation enabled)"""
        try:
            # Format the log entry
            entry = self.format_change_entry(timestamp, summary, changes, archived_files)
            
            if self.obfuscator and password:
                # Encrypted mode: use deterministic file ID
                file_id = self.obfuscator.generate_metadata_file_id(table_name, "changes")
                log_file = self.log_dir / f"{file_id}.enc"
                
                # Read existing encrypted log if it exists
                existing_content = ""
                if log_file.exists():
                    temp_decrypt = self.log_dir / f"{file_id}_temp.log"
                    try:
                        self.obfuscator.encryptor.decrypt_file(log_file, temp_decrypt, password)
                        with open(temp_decrypt, 'r', encoding='utf-8') as f:
                            existing_content = f.read()
                        temp_decrypt.unlink()
                    except Exception as e:
                        logger.error(f"Failed to decrypt existing log for {table_name}: {e}")
                        # Continue with empty content
                
                # Append new entry
                full_content = existing_content + entry + "\n"
                
                # Write to temporary file
                temp_log = self.log_dir / f"{file_id}.log.tmp"
                with open(temp_log, 'w', encoding='utf-8') as f:
                    f.write(full_content)
                
                # Encrypt the file
                self.obfuscator.encryptor.encrypt_file(temp_log, log_file, password)
                
                # Remove temporary file
                temp_log.unlink()
                
                logger.debug(f"Change logged to encrypted file {log_file}")
            else:
                # Non-encrypted mode: use table name
                log_file = self.log_dir / f"{table_name}_changes.log"
                
                # Append to log file
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(entry)
                    f.write("\n")  # Extra newline for readability
                
                logger.debug(f"Change logged to {log_file}")
            
        except Exception as e:
            logger.error(f"Failed to write change log for {table_name}: {e}")
            # Non-fatal - console logging already succeeded
    
    def format_change_entry(
        self,
        timestamp: str,
        summary: str,
        changes: List[Dict],
        archived_files: Optional[Dict[str, Path]] = None
    ) -> str:
        """
        Format a change entry for the log file
        
        Args:
            timestamp: ISO 8601 timestamp
            summary: Human-readable summary
            changes: List of change dictionaries
            archived_files: Dict with 'metadata' and 'ddl' archived file paths
            
        Returns:
            Formatted log entry string
        """
        lines = []
        lines.append(f"[{timestamp}] Schema change detected")
        lines.append(f"Summary: {summary}")
        lines.append("")
        lines.append("Changes:")
        
        for change in changes:
            change_line = self._format_change_line(change)
            lines.append(f"  {change_line}")
        
        if archived_files:
            lines.append("")
            lines.append("Archived Files:")
            if archived_files.get('metadata'):
                lines.append(f"  - {archived_files['metadata']}")
            if archived_files.get('ddl'):
                lines.append(f"  - {archived_files['ddl']}")
        
        lines.append("")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def _format_change_line(self, change: Dict) -> str:
        """Format a single change for the log file"""
        change_type = change['type']
        
        if change_type == 'column_added':
            col = change['column']
            details = change['details']
            data_type = details.get('data_type', 'UNKNOWN')
            nullable = "NULL" if details.get('nullable') else "NOT NULL"
            return f"+ Column added: {col} ({data_type}, {nullable})"
        
        elif change_type == 'column_removed':
            col = change['column']
            details = change['details']
            data_type = details.get('data_type', 'UNKNOWN')
            return f"- Column removed: {col} (was {data_type})"
        
        elif change_type == 'column_type_changed':
            col = change['column']
            details = change['details']
            old_type = details.get('old_type', 'UNKNOWN')
            new_type = details.get('new_type', 'UNKNOWN')
            return f"~ Column type changed: {col}\n      Old: {old_type}\n      New: {new_type}"
        
        elif change_type == 'column_nullable_changed':
            col = change['column']
            details = change['details']
            old_null = "NULL" if details.get('old_nullable') else "NOT NULL"
            new_null = "NULL" if details.get('new_nullable') else "NOT NULL"
            return f"~ Column nullable changed: {col} ({old_null} → {new_null})"
        
        elif change_type == 'column_position_changed':
            col = change['column']
            details = change['details']
            old_pos = details.get('old_position')
            new_pos = details.get('new_position')
            return f"~ Column position changed: {col} (position {old_pos} → {new_pos})"
        
        elif change_type == 'primary_key_changed':
            details = change['details']
            old_pk = ', '.join(details.get('old_pk', []))
            new_pk = ', '.join(details.get('new_pk', []))
            return f"~ Primary key changed: ({old_pk}) → ({new_pk})"
        
        elif change_type == 'foreign_keys_changed':
            details = change['details']
            old_count = details.get('old_count', 0)
            new_count = details.get('new_count', 0)
            return f"~ Foreign keys changed: {old_count} → {new_count}"
        
        elif change_type == 'comment_changed':
            return f"~ Table comment changed"
        
        elif change_type == 'clustering_key_changed':
            return f"~ Clustering key changed"
        
        else:
            return f"~ {change_type}: {change.get('column', 'N/A')}"
    
    def log_initial_extraction(self, table_name: str, created_files: Optional[Dict[str, Path]] = None, password: Optional[str] = None):
        """
        Log initial metadata extraction to console and persistent file
        
        Args:
            table_name: Name of the table
            created_files: Dict with 'metadata' and 'ddl' created file paths
            password: Encryption password (required if obfuscation enabled)
        """
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Log to console (existing behavior)
        logger.info(f"[{timestamp}] Initial metadata extraction for {table_name}")
        
        if created_files:
            if created_files.get('metadata'):
                logger.info(f"  Created: {created_files['metadata']}")
            if created_files.get('ddl'):
                logger.info(f"  Created: {created_files['ddl']}")
        else:
            # Fallback to old format if created_files not provided
            logger.info(f"  Created: {table_name}_metadata.json")
            logger.info(f"  Created: {table_name}_create.sql")
        
        # Write to persistent log file
        self._write_initial_extraction_to_log(table_name, timestamp, created_files, password)
    
    def _write_initial_extraction_to_log(
        self,
        table_name: str,
        timestamp: str,
        created_files: Optional[Dict[str, Path]] = None,
        password: Optional[str] = None
    ):
        """Write initial extraction entry to persistent log file (encrypted if obfuscation enabled)"""
        try:
            lines = []
            lines.append(f"[{timestamp}] Initial metadata extraction")
            lines.append("")
            lines.append("Created Files:")
            
            if created_files:
                if created_files.get('metadata'):
                    lines.append(f"  - {created_files['metadata']}")
                if created_files.get('ddl'):
                    lines.append(f"  - {created_files['ddl']}")
            else:
                lines.append(f"  - {table_name}_metadata.json")
                lines.append(f"  - {table_name}_create.sql")
            
            lines.append("")
            lines.append("=" * 80)
            
            entry = "\n".join(lines)
            
            if self.obfuscator and password:
                # Encrypted mode: use deterministic file ID
                file_id = self.obfuscator.generate_metadata_file_id(table_name, "changes")
                log_file = self.log_dir / f"{file_id}.enc"
                
                # Write to temporary file
                temp_log = self.log_dir / f"{file_id}.log.tmp"
                with open(temp_log, 'w', encoding='utf-8') as f:
                    f.write(entry)
                    f.write("\n")  # Extra newline for readability
                
                # Encrypt the file
                self.obfuscator.encryptor.encrypt_file(temp_log, log_file, password)
                
                # Remove temporary file
                temp_log.unlink()
                
                logger.debug(f"Initial extraction logged to encrypted file {log_file}")
            else:
                # Non-encrypted mode: use table name
                log_file = self.log_dir / f"{table_name}_changes.log"
                
                # Append to log file
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(entry)
                    f.write("\n")  # Extra newline for readability
                
                logger.debug(f"Initial extraction logged to {log_file}")
            
        except Exception as e:
            logger.error(f"Failed to write initial extraction log for {table_name}: {e}")
            # Non-fatal - console logging already succeeded

    
    def get_change_history(
        self,
        table_name: str,
        limit: Optional[int] = None,
        password: Optional[str] = None
    ) -> List[str]:
        """
        Retrieve change history for a table
        
        Args:
            table_name: Name of the table
            limit: Maximum number of entries to return (most recent first)
            password: Decryption password (required if obfuscation enabled)
            
        Returns:
            List of log entries (most recent first)
        """
        if self.obfuscator and password:
            # Encrypted mode: use deterministic file ID
            file_id = self.obfuscator.generate_metadata_file_id(table_name, "changes")
            log_file = self.log_dir / f"{file_id}.enc"
            
            if not log_file.exists():
                logger.info(f"No change log found for {table_name}")
                return []
            
            try:
                # Decrypt to temporary file
                temp_log = self.log_dir / f"{file_id}_temp.log"
                self.obfuscator.encryptor.decrypt_file(log_file, temp_log, password)
                
                # Read content
                with open(temp_log, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Remove temporary file
                temp_log.unlink()
                
            except Exception as e:
                logger.error(f"Failed to read encrypted change log for {table_name}: {e}")
                return []
        else:
            # Non-encrypted mode: use table name
            log_file = self.log_dir / f"{table_name}_changes.log"
            
            if not log_file.exists():
                logger.info(f"No change log found for {table_name}")
                return []
            
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
            except Exception as e:
                logger.error(f"Failed to read change log for {table_name}: {e}")
                return []
        
        # Split by separator
        entries = content.split("=" * 80)
        # Remove empty entries
        entries = [e.strip() for e in entries if e.strip()]
        
        # Reverse to get most recent first
        entries.reverse()
        
        # Apply limit if specified
        if limit and limit > 0:
            entries = entries[:limit]
        
        return entries
    
    def get_changes_by_date_range(
        self,
        table_name: str,
        start_date: datetime,
        end_date: datetime,
        password: Optional[str] = None
    ) -> List[str]:
        """
        Get changes within a date range
        
        Args:
            table_name: Name of the table
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)
            password: Decryption password (required if obfuscation enabled)
            
        Returns:
            List of log entries within the date range
        """
        all_entries = self.get_change_history(table_name, password=password)
        
        filtered_entries = []
        
        for entry in all_entries:
            # Extract timestamp from entry
            timestamp_str = self._extract_timestamp(entry)
            if timestamp_str:
                try:
                    # Parse ISO 8601 timestamp
                    entry_date = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    
                    # Check if within range
                    if start_date <= entry_date <= end_date:
                        filtered_entries.append(entry)
                        
                except Exception as e:
                    logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
                    continue
        
        return filtered_entries
    
    def _extract_timestamp(self, entry: str) -> Optional[str]:
        """Extract timestamp from a log entry"""
        import re
        
        # Match ISO 8601 timestamp in brackets
        match = re.search(r'\[([^\]]+)\]', entry)
        if match:
            return match.group(1)
        return None
    
    def get_change_summary(self, table_name: str) -> Dict[str, any]:
        """
        Get summary of changes for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with change statistics
        """
        log_file = self.log_dir / f"{table_name}_changes.log"
        
        if not log_file.exists():
            return {
                "table_name": table_name,
                "total_changes": 0,
                "has_log": False
            }
        
        entries = self.get_change_history(table_name)
        
        # Count initial extractions vs changes
        initial_count = sum(1 for e in entries if "Initial metadata extraction" in e)
        change_count = len(entries) - initial_count
        
        return {
            "table_name": table_name,
            "total_changes": change_count,
            "initial_extractions": initial_count,
            "total_entries": len(entries),
            "has_log": True,
            "log_file": str(log_file)
        }
    
    def format_change_history(self, entries: List[str]) -> str:
        """
        Format change history for display
        
        Args:
            entries: List of log entries
            
        Returns:
            Formatted string for console output
        """
        if not entries:
            return "No change history found"
        
        lines = []
        lines.append("=" * 80)
        lines.append("CHANGE HISTORY")
        lines.append("=" * 80)
        lines.append("")
        
        for i, entry in enumerate(entries, 1):
            lines.append(f"Entry {i}:")
            lines.append(entry)
            lines.append("")
        
        return "\n".join(lines)
