"""
Metadata Decryptor
Decrypts encrypted metadata files for human viewing
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.transformers.obfuscator import MetadataObfuscator
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class MetadataDecryptor:
    """
    Decrypt encrypted metadata files for human viewing
    
    Features:
    - Decrypt master index
    - Decrypt individual tables or all tables
    - Save to separate decrypted directory (not Git-tracked)
    - Clean up decrypted files
    - Manage .gitignore entries
    """
    
    def __init__(
        self,
        encrypted_dir: Path = None,
        decrypted_dir: Path = None
    ):
        """
        Initialize MetadataDecryptor
        
        Args:
            encrypted_dir: Directory containing encrypted metadata (default: metadata/)
            decrypted_dir: Directory for decrypted files (default: metadata/decrypted/)
        """
        self.encrypted_dir = encrypted_dir or Path("metadata")
        self.decrypted_dir = decrypted_dir or Path("metadata/decrypted")
        
        self.encrypted_schemas_dir = self.encrypted_dir / "schemas"
        self.encrypted_ddl_dir = self.encrypted_dir / "ddl"
        self.encrypted_changes_dir = self.encrypted_dir / "changes"
        self.encrypted_index_file = self.encrypted_dir / "index.enc"
        
        self.decrypted_schemas_dir = self.decrypted_dir / "schemas"
        self.decrypted_ddl_dir = self.decrypted_dir / "ddl"
        self.decrypted_changes_dir = self.decrypted_dir / "changes"
        self.decrypted_index_file = self.decrypted_dir / "index.json"
        
        self.encryptor = FileEncryptor()
        self.obfuscator = MetadataObfuscator()
    
    def decrypt_table(
        self,
        table_name: str,
        password: str
    ) -> Dict[str, Any]:
        """
        Decrypt metadata and DDL for a specific table (without needing index.enc)
        
        Uses deterministic file IDs generated from table name to find files.
        Restores original timestamped filenames for archived versions.
        
        Args:
            table_name: Name of the table
            password: Decryption password
            
        Returns:
            Dictionary with decryption results
        """
        # Ensure decrypted directories exist
        self._ensure_decrypted_directories()
        
        # Generate current file IDs (without timestamp)
        metadata_file_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata")
        ddl_file_id = self.obfuscator.generate_metadata_file_id(table_name, "ddl")
        
        # Decrypt current metadata file
        encrypted_metadata_file = self.encrypted_schemas_dir / f"{metadata_file_id}.enc"
        decrypted_metadata_file = self.decrypted_schemas_dir / f"{table_name}_metadata.json"
        
        if not encrypted_metadata_file.exists():
            raise FileNotFoundError(f"Encrypted metadata file not found: {encrypted_metadata_file}")
        
        logger.info(f"Decrypting current metadata for {table_name}...")
        self.encryptor.decrypt_file(
            encrypted_metadata_file,
            decrypted_metadata_file,
            password
        )
        
        # Decrypt current DDL file
        encrypted_ddl_file = self.encrypted_ddl_dir / f"{ddl_file_id}.enc"
        decrypted_ddl_file = self.decrypted_ddl_dir / f"{table_name}_create.sql"
        
        if not encrypted_ddl_file.exists():
            raise FileNotFoundError(f"Encrypted DDL file not found: {encrypted_ddl_file}")
        
        logger.info(f"Decrypting current DDL for {table_name}...")
        self.encryptor.decrypt_file(
            encrypted_ddl_file,
            decrypted_ddl_file,
            password
        )
        
        # Find and decrypt archived versions (files with timestamps)
        archived_count = 0
        
        # Find archived metadata files (pattern: {base_id}_*.enc where * is not just numbers)
        for encrypted_file in self.encrypted_schemas_dir.glob("*.enc"):
            filename = encrypted_file.stem  # Remove .enc extension
            
            # Check if this is an archived version of our table
            # Archived files have format: {hash}_{timestamp} where hash matches our base ID
            if filename.startswith(metadata_file_id) and len(filename) > len(metadata_file_id):
                # Extract timestamp from filename
                suffix = filename[len(metadata_file_id):]
                if suffix.startswith('_') and len(suffix) == 9:  # _YYYYMMDD
                    timestamp = suffix[1:]  # Remove leading underscore
                    
                    # Verify this is the correct archived file by regenerating its ID
                    expected_id = self.obfuscator.generate_metadata_file_id(table_name, "metadata", timestamp)
                    if filename == expected_id:
                        # Decrypt to timestamped filename
                        decrypted_archived = self.decrypted_schemas_dir / f"{table_name}_metadata_{timestamp}.json"
                        logger.info(f"Decrypting archived metadata: {table_name}_metadata_{timestamp}.json")
                        self.encryptor.decrypt_file(encrypted_file, decrypted_archived, password)
                        archived_count += 1
        
        # Find and decrypt archived DDL files
        for encrypted_file in self.encrypted_ddl_dir.glob("*.enc"):
            filename = encrypted_file.stem
            
            if filename.startswith(ddl_file_id) and len(filename) > len(ddl_file_id):
                suffix = filename[len(ddl_file_id):]
                if suffix.startswith('_') and len(suffix) == 9:
                    timestamp = suffix[1:]
                    
                    expected_id = self.obfuscator.generate_metadata_file_id(table_name, "ddl", timestamp)
                    if filename == expected_id:
                        decrypted_archived = self.decrypted_ddl_dir / f"{table_name}_create_{timestamp}.sql"
                        logger.info(f"Decrypting archived DDL: {table_name}_create_{timestamp}.sql")
                        self.encryptor.decrypt_file(encrypted_file, decrypted_archived, password)
                        archived_count += 1
        
        # Decrypt change log file if it exists
        changes_file_id = self.obfuscator.generate_metadata_file_id(table_name, "changes")
        encrypted_changes_file = self.encrypted_changes_dir / f"{changes_file_id}.enc"
        decrypted_changes_file = self.decrypted_changes_dir / f"{table_name}_changes.log"
        
        has_changes = False
        if encrypted_changes_file.exists():
            logger.info(f"Decrypting change log for {table_name}...")
            self.encryptor.decrypt_file(
                encrypted_changes_file,
                decrypted_changes_file,
                password
            )
            has_changes = True
        
        # Load metadata to get summary
        with open(decrypted_metadata_file, 'r') as f:
            metadata = json.load(f)
        
        result = {
            "table_name": table_name,
            "status": "success",
            "decrypted_files": {
                "metadata": str(decrypted_metadata_file),
                "ddl": str(decrypted_ddl_file),
                "changes": str(decrypted_changes_file) if has_changes else None
            },
            "metadata_summary": {
                "columns": len(metadata.get('columns', [])),
                "row_count": metadata.get('statistics', {}).get('row_count', 0),
                "last_altered": metadata.get('statistics', {}).get('last_altered')
            },
            "has_change_log": has_changes,
            "archived_count": archived_count
        }
        
        logger.info(f"✅ Decrypted {table_name}")
        logger.info(f"   Metadata: {decrypted_metadata_file}")
        logger.info(f"   DDL: {decrypted_ddl_file}")
        if has_changes:
            logger.info(f"   Changes: {decrypted_changes_file}")
        if archived_count > 0:
            logger.info(f"   Archived versions: {archived_count}")
        logger.info(f"   Columns: {result['metadata_summary']['columns']}")
        
        return result
    
    def list_available_tables(self) -> List[str]:
        """
        List all tables from config/tables.yaml
        
        Returns:
            List of table names
        """
        import yaml
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        tables = [t['name'] for t in config['tables']]
        return tables
    
    def clean_decrypted_files(self) -> Dict[str, int]:
        """
        Delete all decrypted files
        
        Returns:
            Dictionary with count of deleted files by type
        """
        import shutil
        
        if not self.decrypted_dir.exists():
            logger.info("No decrypted directory found - nothing to clean")
            return {"deleted_files": 0}
        
        try:
            # Count files before deletion
            metadata_files = list(self.decrypted_schemas_dir.glob("*.json")) if self.decrypted_schemas_dir.exists() else []
            ddl_files = list(self.decrypted_ddl_dir.glob("*.sql")) if self.decrypted_ddl_dir.exists() else []
            changes_files = list(self.decrypted_changes_dir.glob("*.log")) if self.decrypted_changes_dir.exists() else []
            index_files = [self.decrypted_index_file] if self.decrypted_index_file.exists() else []
            
            total_files = len(metadata_files) + len(ddl_files) + len(changes_files) + len(index_files)
            
            # Delete the entire decrypted directory
            shutil.rmtree(self.decrypted_dir)
            
            logger.info(f"✅ Cleaned up {total_files} decrypted file(s)")
            
            return {
                "deleted_files": total_files,
                "metadata_files": len(metadata_files),
                "ddl_files": len(ddl_files),
                "changes_files": len(changes_files),
                "index_files": len(index_files)
            }
            
        except Exception as e:
            logger.error(f"Failed to clean decrypted files: {e}")
            raise
    
    def _ensure_decrypted_directories(self):
        """Create decrypted directory structure if it doesn't exist"""
        try:
            self.decrypted_schemas_dir.mkdir(parents=True, exist_ok=True)
            self.decrypted_ddl_dir.mkdir(parents=True, exist_ok=True)
            self.decrypted_changes_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Decrypted directories ensured: {self.decrypted_dir}")
        except Exception as e:
            raise OSError(f"Cannot create decrypted directory {self.decrypted_dir}: {e}")
    
    def ensure_gitignore(self) -> None:
        """
        Ensure decrypted directory is in .gitignore
        
        Adds 'metadata/decrypted/' to .gitignore if not already present
        """
        gitignore_path = Path(".gitignore")
        decrypted_pattern = "metadata/decrypted/"
        
        # Read existing .gitignore
        existing_lines = []
        if gitignore_path.exists():
            with open(gitignore_path, 'r') as f:
                existing_lines = f.read().splitlines()
        
        # Check if pattern already exists
        if decrypted_pattern in existing_lines:
            logger.debug(f"'{decrypted_pattern}' already in .gitignore")
            return
        
        # Add pattern
        try:
            with open(gitignore_path, 'a') as f:
                if existing_lines and not existing_lines[-1].strip() == '':
                    f.write('\n')  # Add newline if file doesn't end with one
                f.write(f"\n# Decrypted metadata files (temporary, not tracked)\n")
                f.write(f"{decrypted_pattern}\n")
            
            logger.info(f"✅ Added '{decrypted_pattern}' to .gitignore")
            
        except Exception as e:
            logger.error(f"Failed to update .gitignore: {e}")
            raise
