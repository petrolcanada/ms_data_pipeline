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
    
    def decrypt_master_index(self, password: str) -> Dict:
        """
        Decrypt and return master index
        
        Args:
            password: Decryption password
            
        Returns:
            Master index dictionary
            
        Raises:
            FileNotFoundError: If master index doesn't exist
            ValueError: If decryption fails (wrong password or corrupted file)
        """
        if not self.encrypted_index_file.exists():
            raise FileNotFoundError(
                f"Master index not found at {self.encrypted_index_file}. "
                "Run metadata extraction with obfuscation enabled first."
            )
        
        try:
            logger.info(f"Decrypting master index from {self.encrypted_index_file}")
            master_index = self.obfuscator.decrypt_master_index(
                self.encrypted_index_file,
                password
            )
            logger.info(f"✅ Master index decrypted successfully")
            logger.info(f"   Tables: {len(master_index.get('tables', []))}")
            return master_index
            
        except Exception as e:
            if "authentication" in str(e).lower() or "decrypt" in str(e).lower():
                raise ValueError(
                    "Failed to decrypt master index. "
                    "Incorrect password or corrupted file."
                ) from e
            raise
    
    def decrypt_table(
        self,
        table_name: str,
        password: str
    ) -> Dict[str, Any]:
        """
        Decrypt metadata and DDL for a specific table
        
        Args:
            table_name: Name of the table to decrypt
            password: Decryption password
            
        Returns:
            Dictionary with decryption results
            
        Raises:
            FileNotFoundError: If master index doesn't exist
            ValueError: If table not found or decryption fails
        """
        # Decrypt master index to find file IDs
        master_index = self.decrypt_master_index(password)
        
        # Find table in master index
        table_entry = None
        for entry in master_index.get('tables', []):
            if entry.get('table_name') == table_name:
                table_entry = entry
                break
        
        if not table_entry:
            available_tables = [e.get('table_name') for e in master_index.get('tables', [])]
            raise ValueError(
                f"Table '{table_name}' not found in master index. "
                f"Available tables: {', '.join(available_tables)}"
            )
        
        # Get file IDs
        metadata_file_id = table_entry.get('metadata_file_id')
        ddl_file_id = table_entry.get('ddl_file_id')
        
        if not metadata_file_id or not ddl_file_id:
            raise ValueError(f"Invalid table entry for '{table_name}' - missing file IDs")
        
        # Ensure decrypted directories exist
        self._ensure_decrypted_directories()
        
        # Decrypt metadata file
        encrypted_metadata_file = self.encrypted_schemas_dir / f"{metadata_file_id}.enc"
        decrypted_metadata_file = self.decrypted_schemas_dir / f"{table_name}_metadata.json"
        
        if not encrypted_metadata_file.exists():
            raise FileNotFoundError(f"Encrypted metadata file not found: {encrypted_metadata_file}")
        
        logger.info(f"Decrypting metadata for {table_name}...")
        self.encryptor.decrypt_file(
            encrypted_metadata_file,
            decrypted_metadata_file,
            password
        )
        
        # Decrypt DDL file
        encrypted_ddl_file = self.encrypted_ddl_dir / f"{ddl_file_id}.enc"
        decrypted_ddl_file = self.decrypted_ddl_dir / f"{table_name}_create.sql"
        
        if not encrypted_ddl_file.exists():
            raise FileNotFoundError(f"Encrypted DDL file not found: {encrypted_ddl_file}")
        
        logger.info(f"Decrypting DDL for {table_name}...")
        self.encryptor.decrypt_file(
            encrypted_ddl_file,
            decrypted_ddl_file,
            password
        )
        
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
            "has_change_log": has_changes
        }
        
        logger.info(f"✅ Decrypted {table_name}")
        logger.info(f"   Metadata: {decrypted_metadata_file}")
        logger.info(f"   DDL: {decrypted_ddl_file}")
        if has_changes:
            logger.info(f"   Changes: {decrypted_changes_file}")
        logger.info(f"   Columns: {result['metadata_summary']['columns']}")
        
        return result
    
    def decrypt_all_tables(self, password: str) -> Dict[str, Any]:
        """
        Decrypt all tables from master index
        
        Args:
            password: Decryption password
            
        Returns:
            Dictionary with decryption results for each table
        """
        # Decrypt master index
        master_index = self.decrypt_master_index(password)
        
        # Save decrypted master index
        self._ensure_decrypted_directories()
        with open(self.decrypted_index_file, 'w') as f:
            json.dump(master_index, f, indent=2)
        logger.info(f"Saved decrypted master index to {self.decrypted_index_file}")
        
        # Decrypt each table
        results = {}
        tables = master_index.get('tables', [])
        
        logger.info(f"Decrypting {len(tables)} table(s)...")
        
        for table_entry in tables:
            table_name = table_entry.get('table_name')
            
            try:
                result = self.decrypt_table(table_name, password)
                results[table_name] = result
                
            except Exception as e:
                logger.error(f"Failed to decrypt {table_name}: {e}")
                results[table_name] = {
                    "table_name": table_name,
                    "status": "error",
                    "error": str(e)
                }
        
        # Summary
        success_count = sum(1 for r in results.values() if r.get('status') == 'success')
        error_count = len(results) - success_count
        
        logger.info(f"✅ Decryption complete: {success_count} succeeded, {error_count} failed")
        
        return results
    
    def list_available_tables(self, password: str) -> List[str]:
        """
        List all tables in the master index
        
        Args:
            password: Decryption password
            
        Returns:
            List of table names
        """
        master_index = self.decrypt_master_index(password)
        tables = [entry.get('table_name') for entry in master_index.get('tables', [])]
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
