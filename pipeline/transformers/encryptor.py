"""
File Encryptor/Decryptor
Provides secure AES-256-GCM encryption with password-based key derivation
"""
import os
import hashlib
import base64
from pathlib import Path
from typing import Optional
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class FileEncryptor:
    """Encrypt and decrypt files using AES-256-GCM with password-based key derivation"""
    
    def __init__(self, iterations: int = 100000):
        """
        Initialize encryptor
        
        Args:
            iterations: Number of PBKDF2 iterations (higher = more secure but slower)
        """
        self.iterations = iterations
        self.key_length = 32  # 256 bits for AES-256
        
    def generate_salt(self) -> bytes:
        """Generate a random salt for key derivation"""
        return os.urandom(16)  # 128-bit salt
    
    def derive_key(self, password: str, salt: bytes) -> bytes:
        """
        Derive encryption key from password using PBKDF2
        
        Args:
            password: User password
            salt: Random salt
            
        Returns:
            32-byte encryption key
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=self.key_length,
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        return kdf.derive(password.encode('utf-8'))
    
    def encrypt_file(self, input_path: Path, output_path: Path, password: str) -> dict:
        """
        Encrypt a file using AES-256-GCM
        
        Args:
            input_path: Path to input file
            output_path: Path to output encrypted file
            password: Encryption password
            
        Returns:
            Dictionary with encryption metadata (salt, checksum)
        """
        try:
            # Generate salt
            salt = self.generate_salt()
            
            # Derive key from password
            key = self.derive_key(password, salt)
            
            # Read input file
            with open(input_path, 'rb') as f:
                plaintext = f.read()
            
            # Calculate checksum of original file
            checksum = hashlib.sha256(plaintext).hexdigest()
            
            # Encrypt using AES-GCM
            aesgcm = AESGCM(key)
            nonce = os.urandom(12)  # 96-bit nonce for GCM
            ciphertext = aesgcm.encrypt(nonce, plaintext, None)
            
            # Write encrypted file: salt + nonce + ciphertext
            with open(output_path, 'wb') as f:
                f.write(salt)
                f.write(nonce)
                f.write(ciphertext)
            
            logger.info(f"Encrypted {input_path.name} -> {output_path.name}")
            logger.debug(f"  Original size: {len(plaintext):,} bytes")
            logger.debug(f"  Encrypted size: {len(salt) + len(nonce) + len(ciphertext):,} bytes")
            
            return {
                "salt": base64.b64encode(salt).decode('utf-8'),
                "checksum_sha256": checksum,
                "original_size": len(plaintext),
                "encrypted_size": len(salt) + len(nonce) + len(ciphertext)
            }
            
        except Exception as e:
            logger.error(f"Failed to encrypt {input_path}: {e}")
            raise
    
    def decrypt_file(self, input_path: Path, output_path: Path, password: str) -> dict:
        """
        Decrypt a file using AES-256-GCM
        
        Args:
            input_path: Path to encrypted file
            output_path: Path to output decrypted file
            password: Decryption password
            
        Returns:
            Dictionary with decryption metadata (checksum)
        """
        try:
            # Read encrypted file
            with open(input_path, 'rb') as f:
                # Read salt (16 bytes)
                salt = f.read(16)
                # Read nonce (12 bytes)
                nonce = f.read(12)
                # Read ciphertext (rest of file)
                ciphertext = f.read()
            
            # Derive key from password
            key = self.derive_key(password, salt)
            
            # Decrypt using AES-GCM
            aesgcm = AESGCM(key)
            plaintext = aesgcm.decrypt(nonce, ciphertext, None)
            
            # Calculate checksum of decrypted file
            checksum = hashlib.sha256(plaintext).hexdigest()
            
            # Write decrypted file
            with open(output_path, 'wb') as f:
                f.write(plaintext)
            
            logger.info(f"Decrypted {input_path.name} -> {output_path.name}")
            logger.debug(f"  Encrypted size: {len(salt) + len(nonce) + len(ciphertext):,} bytes")
            logger.debug(f"  Decrypted size: {len(plaintext):,} bytes")
            
            return {
                "checksum_sha256": checksum,
                "decrypted_size": len(plaintext)
            }
            
        except Exception as e:
            logger.error(f"Failed to decrypt {input_path}: {e}")
            if "authentication" in str(e).lower():
                logger.error("Decryption failed - wrong password or corrupted file")
            raise
    
    def verify_checksum(self, file_path: Path, expected_checksum: str) -> bool:
        """
        Verify file checksum
        
        Args:
            file_path: Path to file
            expected_checksum: Expected SHA-256 checksum
            
        Returns:
            True if checksum matches
        """
        try:
            with open(file_path, 'rb') as f:
                actual_checksum = hashlib.sha256(f.read()).hexdigest()
            
            matches = actual_checksum == expected_checksum
            
            if matches:
                logger.info(f"Checksum verified for {file_path.name}")
            else:
                logger.error(f"Checksum mismatch for {file_path.name}")
                logger.error(f"  Expected: {expected_checksum}")
                logger.error(f"  Actual: {actual_checksum}")
            
            return matches
            
        except Exception as e:
            logger.error(f"Failed to verify checksum for {file_path}: {e}")
            return False
