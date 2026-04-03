"""
Data Export Obfuscator
Generates random identifiers for folders and files to enhance security.

Includes salted deterministic IDs, secure temp-file handling, and encrypted
master indexes.
"""
import os
import secrets
import json
import tempfile
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class DataObfuscator:
    """
    Generates obfuscated identifiers for export folders and files
    
    Features:
    - Cryptographically secure random identifiers
    - Hexadecimal format (lowercase)
    - Uniqueness verification
    - Master index creation and encryption
    """
    
    def __init__(self, identifier_length: int = 16, salt: Optional[str] = None):
        """
        Initialize obfuscator.

        Args:
            identifier_length: Length of generated identifiers (default: 16 chars).
            salt: Secret salt mixed into deterministic ID generation.
                  Falls back to OBFUSCATION_SALT from settings if not provided.
        """
        self.identifier_length = identifier_length
        self._used_identifiers: set = set()
        self.encryptor = FileEncryptor()

        if salt is not None:
            self._salt = salt
        else:
            try:
                from pipeline.config.settings import get_settings
                self._salt = get_settings().obfuscation_salt or ""
            except Exception:
                self._salt = ""
    
    def generate_identifier(self) -> str:
        """
        Generate a cryptographically secure random identifier
        
        Returns:
            Hexadecimal string identifier
        """
        max_attempts = 100
        for _ in range(max_attempts):
            # Generate random bytes and convert to hex
            random_bytes = secrets.token_bytes(self.identifier_length // 2)
            identifier = random_bytes.hex()
            
            # Verify uniqueness
            if identifier not in self._used_identifiers:
                self._used_identifiers.add(identifier)
                logger.debug(f"Generated unique identifier: {identifier}")
                return identifier
        
        raise RuntimeError(f"Failed to generate unique identifier after {max_attempts} attempts")
    
    def generate_deterministic_identifier(self, key: str, context: str = "") -> str:
        """
        Generate a deterministic identifier based on a key.

        When an obfuscation salt is configured (OBFUSCATION_SALT env var or
        passed at init), the salt is mixed in so that the mapping is not
        reproducible without the secret.
        """
        import hashlib

        salt_prefix = self._salt or ""
        data = f"{salt_prefix}:{key}:{context}".encode("utf-8")
        hash_obj = hashlib.sha256(data)
        identifier = hash_obj.hexdigest()[: self.identifier_length]

        logger.debug(f"Generated deterministic identifier (context: '{context}')")
        return identifier
    
    def generate_folder_id(self, table_name: str) -> str:
        """
        Generate deterministic obfuscated folder identifier for a table
        
        Uses deterministic generation so the same table always gets
        the same folder ID across multiple runs.
        
        Args:
            table_name: Original table name
            
        Returns:
            Deterministic folder identifier
        """
        folder_id = self.generate_deterministic_identifier(table_name, "folder")
        logger.debug(f"Generated folder ID for table: {folder_id}")
        return folder_id
    
    def generate_file_id(self, chunk_number: int) -> str:
        """
        Generate obfuscated file identifier for a chunk (DEPRECATED - use generate_chunk_file_id)
        
        This method generates random IDs and is kept for backward compatibility.
        New code should use generate_chunk_file_id() for deterministic IDs.
        
        Args:
            chunk_number: Chunk number (for logging only)
            
        Returns:
            Random file identifier
        """
        file_id = self.generate_identifier()
        logger.debug(f"Generated random file ID for chunk {chunk_number}: {file_id}")
        return file_id
    
    def generate_chunk_file_id(self, table_name: str, chunk_number: int) -> str:
        """
        Generate deterministic obfuscated file identifier for a data chunk
        
        Uses deterministic generation so the same table and chunk number
        always produce the same file ID across multiple runs.
        
        Args:
            table_name: Original table name
            chunk_number: Chunk number (1-indexed)
            
        Returns:
            Deterministic file identifier
        """
        # Create deterministic ID from table name and chunk number
        key = f"{table_name}:chunk:{chunk_number}"
        file_id = self.generate_deterministic_identifier(key, "data")
        logger.debug(f"Generated deterministic chunk file ID for '{table_name}' chunk {chunk_number}: {file_id}")
        return file_id
    
    def generate_manifest_id(self, table_name: str) -> str:
        """
        Generate deterministic obfuscated manifest file identifier for a table
        
        Uses deterministic generation so the same table always gets
        the same manifest ID across multiple runs.
        
        Args:
            table_name: Original table name
            
        Returns:
            Deterministic manifest file identifier
        """
        manifest_id = self.generate_deterministic_identifier(table_name, "manifest")
        logger.debug(f"Generated manifest ID: {manifest_id}")
        return manifest_id
    
    def create_master_index(
        self,
        table_mappings: List[Dict],
        output_path: Path,
        password: str
    ) -> Dict:
        """
        Create and encrypt master index file
        
        Args:
            table_mappings: List of table mapping dictionaries
                          Each should contain: table_name, folder_id, manifest_file_id, etc.
            output_path: Path to save encrypted index
            password: Encryption password
            
        Returns:
            Master index metadata
        """
        try:
            # Create master index structure
            master_index = {
                "version": "1.0",
                "created_at": datetime.utcnow().isoformat() + "Z",
                "obfuscation_enabled": True,
                "tables": table_mappings
            }
            
            logger.info(f"Creating master index with {len(table_mappings)} table(s)")
            
            # Use secure temp file with restrictive permissions
            fd, tmp_path_str = tempfile.mkstemp(suffix=".json", dir=str(output_path.parent))
            temp_json = Path(tmp_path_str)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(master_index, f, indent=2)
                
                logger.info("Encrypting master index...")
                encryption_info = self.encryptor.encrypt_file(temp_json, output_path, password)
            finally:
                if temp_json.exists():
                    temp_json.unlink()
            
            logger.info(f"Master index created and encrypted: {output_path}")
            logger.info(f"  Size: {encryption_info['encrypted_size'] / 1024:.2f} KB")
            
            return {
                "file": output_path.name,
                "size_bytes": encryption_info['encrypted_size'],
                "checksum_sha256": encryption_info['checksum_sha256'],
                "table_count": len(table_mappings)
            }
            
        except Exception as e:
            logger.error(f"Failed to create master index: {e}")
            raise
    
    def decrypt_master_index(
        self,
        index_path: Path,
        password: str
    ) -> Dict:
        """
        Decrypt and load master index file
        
        Args:
            index_path: Path to encrypted index file
            password: Decryption password
            
        Returns:
            Master index dictionary
        """
        try:
            logger.info(f"Decrypting master index: {index_path}")
            
            fd, tmp_path_str = tempfile.mkstemp(suffix=".json", dir=str(index_path.parent))
            temp_json = Path(tmp_path_str)
            os.close(fd)
            try:
                self.encryptor.decrypt_file(index_path, temp_json, password)
                with open(temp_json, "r") as f:
                    master_index = json.load(f)
            finally:
                if temp_json.exists():
                    temp_json.unlink()
            
            logger.info(f"Master index decrypted successfully")
            logger.info(f"  Version: {master_index.get('version')}")
            logger.info(f"  Tables: {len(master_index.get('tables', []))}")
            
            return master_index
            
        except Exception as e:
            logger.error(f"Failed to decrypt master index: {e}")
            raise ValueError(
                "Failed to decrypt master index. "
                "Verify password is correct and file is not corrupted."
            )
    
    def find_table_folder(
        self,
        master_index: Dict,
        table_name: str
    ) -> Optional[str]:
        """
        Find obfuscated folder ID for a table name
        
        Args:
            master_index: Decrypted master index
            table_name: Table name to find
            
        Returns:
            Folder ID or None if not found
        """
        for table_entry in master_index.get('tables', []):
            if table_entry.get('table_name') == table_name:
                folder_id = table_entry.get('folder_id')
                logger.debug(f"Found folder ID for '{table_name}': {folder_id}")
                return folder_id
        
        logger.warning(f"Table '{table_name}' not found in master index")
        return None
    
    def reset(self):
        """Reset used identifiers (for new export session)"""
        self._used_identifiers.clear()
        logger.debug("Obfuscator reset - cleared used identifiers")



class MetadataObfuscator(DataObfuscator):
    """
    Obfuscator specifically for metadata files (JSON and DDL)
    
    Extends DataObfuscator with metadata-specific functionality
    """
    
    def generate_metadata_file_id(self, table_name: str, file_type: str, timestamp: str = None) -> str:
        """
        Generate deterministic obfuscated file identifier for metadata
        
        Uses deterministic generation so the same table always gets
        the same file ID across multiple runs.
        
        Args:
            table_name: Original table name
            file_type: Type of file ('metadata', 'ddl', or 'changes')
            timestamp: Optional timestamp (YYYYMMDD format) for archived files
            
        Returns:
            Deterministic file identifier
        """
        # If timestamp provided, include it in the key for archived files
        # Format: table_name:file_type:timestamp for archived files
        if timestamp:
            key = f"{table_name}:{file_type}:{timestamp}"
        else:
            key = table_name
        
        file_id = self.generate_deterministic_identifier(key, file_type if not timestamp else "")
        
        if timestamp:
            logger.debug(f"Generated {file_type} file ID with timestamp {timestamp}: {file_id}")
        else:
            logger.debug(f"Generated {file_type} file ID: {file_id}")
        
        return file_id
    
    def create_metadata_master_index(
        self,
        table_mappings: List[Dict],
        output_path: Path,
        password: str
    ) -> Dict:
        """
        Create and encrypt master index for metadata files
        
        Args:
            table_mappings: List of table mapping dictionaries
                          Each should contain: table_name, metadata_file_id, ddl_file_id
            output_path: Path to save encrypted index (metadata/index.enc)
            password: Encryption password
            
        Returns:
            Master index metadata
        """
        try:
            # Create master index structure
            master_index = {
                "version": "1.0",
                "created_at": datetime.utcnow().isoformat() + "Z",
                "obfuscation_enabled": True,
                "type": "metadata",
                "tables": table_mappings
            }
            
            logger.info(f"Creating metadata master index with {len(table_mappings)} table(s)")
            
            fd, tmp_path_str = tempfile.mkstemp(suffix=".json", dir=str(output_path.parent))
            temp_json = Path(tmp_path_str)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(master_index, f, indent=2)
                
                logger.info("Encrypting metadata master index...")
                encryption_info = self.encryptor.encrypt_file(temp_json, output_path, password)
            finally:
                if temp_json.exists():
                    temp_json.unlink()
            
            logger.info(f"Metadata master index created and encrypted: {output_path}")
            logger.info(f"  Size: {encryption_info['encrypted_size'] / 1024:.2f} KB")
            
            return {
                "file": output_path.name,
                "size_bytes": encryption_info['encrypted_size'],
                "checksum_sha256": encryption_info['checksum_sha256'],
                "table_count": len(table_mappings)
            }
            
        except Exception as e:
            logger.error(f"Failed to create metadata master index: {e}")
            raise
    
    def find_metadata_files(
        self,
        master_index: Dict,
        table_name: str
    ) -> Optional[Dict]:
        """
        Find obfuscated file IDs for a table's metadata
        
        Args:
            master_index: Decrypted master index
            table_name: Table name to find
            
        Returns:
            Dictionary with metadata_file_id and ddl_file_id, or None if not found
        """
        for table_entry in master_index.get('tables', []):
            if table_entry.get('table_name') == table_name:
                result = {
                    'metadata_file_id': table_entry.get('metadata_file_id'),
                    'ddl_file_id': table_entry.get('ddl_file_id')
                }
                logger.debug(f"Found metadata files for '{table_name}': {result}")
                return result
        
        logger.warning(f"Table '{table_name}' not found in metadata master index")
        return None
