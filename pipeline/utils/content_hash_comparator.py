"""
Content Hash Comparator
Provides content-based change detection for export files using SHA-256 hashing
"""
import hashlib
from pathlib import Path
from typing import Optional, Tuple
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class ContentHashComparator:
    """
    Utility for content-based change detection using SHA-256 hashes.
    
    This class helps determine if files need to be written by comparing
    content hashes of new data with existing encrypted files.
    """
    
    def __init__(self, encryptor: FileEncryptor):
        """
        Initialize with an encryptor for decrypting existing files.
        
        Args:
            encryptor: FileEncryptor instance for decryption operations
        """
        self.encryptor = encryptor
        self.logger = logger
    
    def compute_file_hash(self, file_path: Path) -> str:
        """
        Compute SHA-256 hash of a file's contents.
        
        Reads the file in chunks to handle large files efficiently.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hexadecimal string representation of SHA-256 hash
            
        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If file cannot be read
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            sha256_hash = hashlib.sha256()
            
            # Read file in chunks to handle large files
            with open(file_path, 'rb') as f:
                # Read in 64KB chunks
                for chunk in iter(lambda: f.read(65536), b''):
                    sha256_hash.update(chunk)
            
            hash_value = sha256_hash.hexdigest()
            self.logger.debug(f"Computed hash for {file_path.name}: {hash_value[:16]}...")
            
            return hash_value
            
        except IOError as e:
            self.logger.error(f"Failed to read {file_path}: {e}")
            raise
    
    def decrypt_and_hash(
        self, 
        encrypted_file: Path, 
        password: str
    ) -> Optional[str]:
        """
        Decrypt an encrypted file and compute its content hash.
        
        Creates a temporary decrypted file, computes hash, then deletes temp file.
        This ensures we don't leave unencrypted data on disk.
        
        Args:
            encrypted_file: Path to encrypted file
            password: Decryption password
            
        Returns:
            SHA-256 hash of decrypted content, or None if decryption fails
        """
        if not encrypted_file.exists():
            self.logger.debug(f"Encrypted file does not exist: {encrypted_file.name}")
            return None
        
        # Create temporary file path for decryption
        temp_file = encrypted_file.parent / f".tmp_{encrypted_file.stem}_decrypted"
        
        try:
            # Decrypt to temporary file
            self.logger.debug(f"Decrypting {encrypted_file.name} for comparison...")
            self.encryptor.decrypt_file(encrypted_file, temp_file, password)
            
            # Compute hash of decrypted content
            content_hash = self.compute_file_hash(temp_file)
            
            self.logger.debug(f"Existing file hash: {content_hash[:16]}...")
            
            return content_hash
            
        except Exception as e:
            self.logger.warning(
                f"Failed to decrypt and hash {encrypted_file.name}: {e}"
            )
            return None
            
        finally:
            # Always clean up temporary file
            if temp_file.exists():
                try:
                    temp_file.unlink()
                    self.logger.debug(f"Cleaned up temporary file: {temp_file.name}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete temp file {temp_file.name}: {e}")
    
    def should_write_file(
        self,
        new_content_hash: str,
        target_encrypted_file: Path,
        password: str
    ) -> Tuple[bool, str]:
        """
        Determine if a file should be written based on content comparison.
        
        Compares the hash of new content with the hash of existing encrypted file
        (after decryption). Returns decision and reason.
        
        Args:
            new_content_hash: Hash of the new content to be written
            target_encrypted_file: Path where encrypted file would be written
            password: Password for decrypting existing file
            
        Returns:
            Tuple of (should_write: bool, reason: str)
            - (True, "new_file"): File doesn't exist, should write
            - (True, "content_changed"): Content differs, should write
            - (True, "decryption_failed"): Couldn't decrypt existing, should write
            - (False, "content_unchanged"): Content identical, skip write
        """
        # Check if file exists
        if not target_encrypted_file.exists():
            self.logger.debug(
                f"File {target_encrypted_file.name} does not exist - will write"
            )
            return (True, "new_file")
        
        # Decrypt existing file and compute hash
        existing_hash = self.decrypt_and_hash(target_encrypted_file, password)
        
        # If decryption failed, write the new file
        if existing_hash is None:
            self.logger.warning(
                f"Could not decrypt {target_encrypted_file.name} - will overwrite"
            )
            return (True, "decryption_failed")
        
        # Compare hashes
        if new_content_hash == existing_hash:
            self.logger.info(
                f"Content unchanged for {target_encrypted_file.name} - skipping write"
            )
            return (False, "content_unchanged")
        else:
            self.logger.info(
                f"Content changed for {target_encrypted_file.name} - will write"
            )
            return (True, "content_changed")
