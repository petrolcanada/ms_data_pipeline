"""
Unit tests for ContentHashComparator
"""
import sys
from pathlib import Path
import tempfile
import hashlib

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from pipeline.utils.content_hash_comparator import ContentHashComparator
from pipeline.transformers.encryptor import FileEncryptor


def test_compute_file_hash_consistency():
    """Test that computing hash twice on same file gives same result"""
    comparator = ContentHashComparator(FileEncryptor())
    
    # Create a temporary file with known content
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Test content for hashing")
        temp_file = Path(f.name)
    
    try:
        # Compute hash twice
        hash1 = comparator.compute_file_hash(temp_file)
        hash2 = comparator.compute_file_hash(temp_file)
        
        # Should be identical
        assert hash1 == hash2, "Hash should be consistent"
        
        # Verify it's a valid SHA-256 hash (64 hex characters)
        assert len(hash1) == 64, "SHA-256 hash should be 64 characters"
        assert all(c in '0123456789abcdef' for c in hash1), "Hash should be hexadecimal"
        
        print("✅ test_compute_file_hash_consistency passed")
    finally:
        temp_file.unlink()


def test_compute_file_hash_different_content():
    """Test that different files produce different hashes"""
    comparator = ContentHashComparator(FileEncryptor())
    
    # Create two temporary files with different content
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Content A")
        temp_file1 = Path(f.name)
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Content B")
        temp_file2 = Path(f.name)
    
    try:
        hash1 = comparator.compute_file_hash(temp_file1)
        hash2 = comparator.compute_file_hash(temp_file2)
        
        # Should be different
        assert hash1 != hash2, "Different content should produce different hashes"
        
        print("✅ test_compute_file_hash_different_content passed")
    finally:
        temp_file1.unlink()
        temp_file2.unlink()


def test_should_write_file_new():
    """Test that should_write_file returns True for non-existent files"""
    comparator = ContentHashComparator(FileEncryptor())
    
    # Create a hash for new content
    new_hash = hashlib.sha256(b"new content").hexdigest()
    
    # Use a path that doesn't exist
    non_existent = Path(tempfile.gettempdir()) / "non_existent_file.enc"
    
    should_write, reason = comparator.should_write_file(
        new_hash,
        non_existent,
        "test_password"
    )
    
    assert should_write == True, "Should write new file"
    assert reason == "new_file", "Reason should be 'new_file'"
    
    print("✅ test_should_write_file_new passed")


def test_decrypt_and_hash_round_trip():
    """Test that encrypting and then decrypting produces same hash"""
    encryptor = FileEncryptor()
    comparator = ContentHashComparator(encryptor)
    password = "test_password_123"
    
    # Create a temporary file with content
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Test content for encryption round trip")
        original_file = Path(f.name)
    
    encrypted_file = Path(tempfile.gettempdir()) / "test_encrypted.enc"
    
    try:
        # Compute hash of original
        original_hash = comparator.compute_file_hash(original_file)
        
        # Encrypt the file
        encryptor.encrypt_file(original_file, encrypted_file, password)
        
        # Decrypt and hash
        decrypted_hash = comparator.decrypt_and_hash(encrypted_file, password)
        
        # Should match original
        assert decrypted_hash == original_hash, "Round-trip hash should match original"
        
        print("✅ test_decrypt_and_hash_round_trip passed")
    finally:
        if original_file.exists():
            original_file.unlink()
        if encrypted_file.exists():
            encrypted_file.unlink()


def test_should_write_file_unchanged():
    """Test that should_write_file returns False for unchanged content"""
    encryptor = FileEncryptor()
    comparator = ContentHashComparator(encryptor)
    password = "test_password_123"
    
    # Create a file with content
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Unchanged content")
        original_file = Path(f.name)
    
    encrypted_file = Path(tempfile.gettempdir()) / "test_unchanged.enc"
    
    try:
        # Compute hash and encrypt
        content_hash = comparator.compute_file_hash(original_file)
        encryptor.encrypt_file(original_file, encrypted_file, password)
        
        # Check if we should write (same content)
        should_write, reason = comparator.should_write_file(
            content_hash,
            encrypted_file,
            password
        )
        
        assert should_write == False, "Should not write unchanged file"
        assert reason == "content_unchanged", "Reason should be 'content_unchanged'"
        
        print("✅ test_should_write_file_unchanged passed")
    finally:
        if original_file.exists():
            original_file.unlink()
        if encrypted_file.exists():
            encrypted_file.unlink()


def test_should_write_file_changed():
    """Test that should_write_file returns True for changed content"""
    encryptor = FileEncryptor()
    comparator = ContentHashComparator(encryptor)
    password = "test_password_123"
    
    # Create original file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Original content")
        original_file = Path(f.name)
    
    # Create changed file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Changed content")
        changed_file = Path(f.name)
    
    encrypted_file = Path(tempfile.gettempdir()) / "test_changed.enc"
    
    try:
        # Encrypt original
        encryptor.encrypt_file(original_file, encrypted_file, password)
        
        # Compute hash of changed content
        changed_hash = comparator.compute_file_hash(changed_file)
        
        # Check if we should write (different content)
        should_write, reason = comparator.should_write_file(
            changed_hash,
            encrypted_file,
            password
        )
        
        assert should_write == True, "Should write changed file"
        assert reason == "content_changed", "Reason should be 'content_changed'"
        
        print("✅ test_should_write_file_changed passed")
    finally:
        if original_file.exists():
            original_file.unlink()
        if changed_file.exists():
            changed_file.unlink()
        if encrypted_file.exists():
            encrypted_file.unlink()


if __name__ == "__main__":
    print("Running ContentHashComparator tests...\n")
    
    try:
        test_compute_file_hash_consistency()
        test_compute_file_hash_different_content()
        test_should_write_file_new()
        test_decrypt_and_hash_round_trip()
        test_should_write_file_unchanged()
        test_should_write_file_changed()
        
        print("\n" + "=" * 50)
        print("✅ All tests passed!")
        print("=" * 50)
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
