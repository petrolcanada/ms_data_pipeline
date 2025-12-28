"""
Data Export Obfuscator
Generates random identifiers for folders and files to enhance security
"""
import secrets
import json
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
    
    def __init__(self, identifier_length: int = 16):
        """
        Initialize obfuscator
        
        Args:
            identifier_length: Length of generated identifiers (default: 16 chars)
        """
        self.identifier_length = identifier_length
        self._used_identifiers = set()
        self.encryptor = FileEncryptor()
    
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
    
    def generate_folder_id(self, table_name: str) -> str:
        """
        Generate obfuscated folder identifier for a table
        
        Args:
            table_name: Original table name
            
        Returns:
            Random folder identifier
        """
        folder_id = self.generate_identifier()
        logger.info(f"Generated folder ID for table '{table_name}': {folder_id}")
        return folder_id
    
    def generate_file_id(self, chunk_number: int) -> str:
        """
        Generate obfuscated file identifier for a chunk
        
        Args:
            chunk_number: Chunk number (for logging only)
            
        Returns:
            Random file identifier
        """
        file_id = self.generate_identifier()
        logger.debug(f"Generated file ID for chunk {chunk_number}: {file_id}")
        return file_id
    
    def generate_manifest_id(self, table_name: str) -> str:
        """
        Generate obfuscated manifest file identifier for a table
        
        Args:
            table_name: Original table name (for logging only)
            
        Returns:
            Random manifest file identifier
        """
        manifest_id = self.generate_identifier()
        logger.info(f"Generated manifest ID for table '{table_name}': {manifest_id}")
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
            
            # Save as temporary JSON file
            temp_json = output_path.parent / "index.json.tmp"
            with open(temp_json, 'w') as f:
                json.dump(master_index, f, indent=2)
            
            # Encrypt the index file
            logger.info("Encrypting master index...")
            encryption_info = self.encryptor.encrypt_file(
                temp_json,
                output_path,
                password
            )
            
            # Remove temporary file
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
            
            # Decrypt the index file
            temp_json = index_path.parent / "index.json.tmp"
            self.encryptor.decrypt_file(
                index_path,
                temp_json,
                password
            )
            
            # Load JSON
            with open(temp_json, 'r') as f:
                master_index = json.load(f)
            
            # Remove temporary file
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
