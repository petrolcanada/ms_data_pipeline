#!/usr/bin/env python3
"""
Data Import Script - Phase 2
Decrypt and load data from encrypted files to PostgreSQL

Usage:
    python scripts/import_data.py --table financial_data
    python scripts/import_data.py --table financial_data --password-file ~/.encryption_key
    python scripts/import_data.py --all
"""
import sys
import json
import argparse
import getpass
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.transformers.encryptor import FileEncryptor
from pipeline.transformers.obfuscator import DataObfuscator
from pipeline.loaders.data_loader import PostgreSQLDataLoader
from pipeline.config.settings import get_settings
from pipeline.utils.logger import get_logger
import yaml
import tempfile

logger = get_logger(__name__)


def get_password(password_file: str = None, from_env: str = None) -> str:
    """
    Get decryption password from environment, file, or prompt
    
    Priority: password_file > from_env > prompt
    """
    # Try password file first
    if password_file:
        password_path = Path(password_file).expanduser()
        if password_path.exists():
            with open(password_path, 'r') as f:
                password = f.read().strip()
            logger.info(f"Password loaded from {password_file}")
            return password
        else:
            logger.warning(f"Password file not found: {password_file}")
    
    # Try environment variable
    if from_env:
        logger.info("Using decryption password from .env")
        return from_env
    
    # Prompt for password
    password = getpass.getpass("Enter decryption password: ")
    return password


def import_table(
    table_config: dict,
    password: str,
    import_base_dir: str,
    truncate_first: bool = False,
    keep_decrypted: bool = False
):
    """
    Import a single table
    
    Args:
        table_config: Table configuration from tables.yaml
        password: Decryption password
        import_base_dir: Base import directory
        truncate_first: Truncate table before loading
        keep_decrypted: Keep decrypted Parquet files
    """
    table_name = table_config['name']
    pg_config = table_config['postgres']
    
    print("\n" + "=" * 70)
    print(f"IMPORTING TABLE: {table_name}")
    print("=" * 70)
    
    # Try to find import directory (obfuscated or not)
    import_base_path = Path(import_base_dir)
    import_dir = None
    obfuscated = False
    
    # First, try human-readable folder name
    candidate_dir = import_base_path / table_name
    if candidate_dir.exists():
        import_dir = candidate_dir
        print(f"📁 Found folder: {table_name} (non-obfuscated)")
    else:
        # Try obfuscated folder ID
        obfuscator = DataObfuscator()
        folder_id = obfuscator.generate_folder_id(table_name)
        candidate_dir = import_base_path / folder_id
        
        if candidate_dir.exists():
            import_dir = candidate_dir
            obfuscated = True
            print(f"🔒 Found obfuscated folder: {folder_id}")
            print(f"   (Deterministic folder ID for {table_name})")
        else:
            raise FileNotFoundError(
                f"Import directory not found for table: {table_name}\n"
                f"   Tried: {import_base_path / table_name}\n"
                f"   Tried: {import_base_path / folder_id}\n"
                f"   Make sure data is transferred to the import directory"
            )
    
    logger.info(f"Import directory: {import_dir}")
    
    # Read manifest (might be encrypted if obfuscated)
    manifest = None
    manifest_file = import_dir / "manifest.json"
    
    if manifest_file.exists():
        # Plain manifest
        print(f"📋 Reading manifest: manifest.json")
        with open(manifest_file, 'r') as f:
            manifest = json.load(f)
    else:
        # Look for encrypted manifest
        if obfuscated:
            print(f"🔒 Manifest is encrypted, searching for encrypted manifest...")
            
            # Find any .enc file that might be the manifest
            enc_files = list(import_dir.glob("*.enc"))
            
            # Try to decrypt each .enc file to find the manifest
            # The manifest should be the smallest file or have specific content
            manifest_found = False
            
            for enc_file in enc_files:
                # Skip data chunk files (they're larger)
                if enc_file.stat().st_size > 1024 * 1024:  # Skip files > 1MB
                    continue
                
                try:
                    print(f"   Trying to decrypt: {enc_file.name}")
                    
                    # Decrypt to temporary file
                    with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as temp_file:
                        temp_path = Path(temp_file.name)
                    
                    encryptor = FileEncryptor()
                    encryptor.decrypt_file(enc_file, temp_path, password)
                    
                    # Try to read as JSON
                    with open(temp_path, 'r') as f:
                        content = json.load(f)
                    
                    # Check if it's a manifest (has table_name field)
                    if 'table_name' in content and content['table_name'] == table_name:
                        manifest = content
                        manifest_found = True
                        print(f"   ✅ Found and decrypted manifest: {enc_file.name}")
                        temp_path.unlink()  # Clean up temp file
                        break
                    
                    temp_path.unlink()  # Clean up temp file
                    
                except Exception as e:
                    # Not the manifest or wrong password
                    if temp_path.exists():
                        temp_path.unlink()
                    continue
            
            if not manifest_found:
                raise FileNotFoundError(
                    f"Encrypted manifest not found for table: {table_name}\n"
                    f"   Searched in: {import_dir}\n"
                    f"   Make sure the manifest file is transferred"
                )
        else:
            raise FileNotFoundError(f"Manifest file not found: {manifest_file}")
    
    print(f"\n📋 Manifest loaded:")
    print(f"   Export date: {manifest['export_timestamp']}")
    print(f"   Total rows: {manifest['total_rows']:,}")
    print(f"   Total chunks: {manifest['total_chunks']}")
    
    # Display filter information if present
    if manifest.get('snowflake_source', {}).get('filter'):
        print(f"   Filter used: {manifest['snowflake_source']['filter']}")
    else:
        print(f"   Filter: None (all data exported)")
    
    # Initialize components
    encryptor = FileEncryptor()
    loader = PostgreSQLDataLoader()
    
    # Truncate table if requested
    if truncate_first:
        print(f"\n🗑️  Truncating {pg_config['schema']}.{pg_config['table']}...")
        loader.truncate_table(pg_config['schema'], pg_config['table'])
    
    # Process chunks
    print(f"\n🔄 Processing {manifest['total_chunks']} chunks...")
    
    total_loaded = 0
    temp_files = []
    
    for chunk_info in manifest['chunks']:
        chunk_num = chunk_info['chunk_number']
        encrypted_file = import_dir / chunk_info['file']
        
        print(f"\n📦 Chunk {chunk_num}/{manifest['total_chunks']}:")
        print(f"   File: {encrypted_file.name}")
        print(f"   Rows: {chunk_info['rows']:,}")
        
        # Verify encrypted file exists
        if not encrypted_file.exists():
            raise FileNotFoundError(f"Encrypted file not found: {encrypted_file}")
        
        # Decrypt file
        decrypted_file = import_dir / f"data_chunk_{chunk_num:03d}.parquet"
        print(f"   🔓 Decrypting...")
        
        try:
            decryption_info = encryptor.decrypt_file(
                encrypted_file,
                decrypted_file,
                password
            )
        except Exception as e:
            if "authentication" in str(e).lower():
                print(f"\n❌ Decryption failed - wrong password or corrupted file")
                raise ValueError("Wrong password or corrupted file")
            raise
        
        # Verify checksum
        print(f"   ✅ Verifying checksum...")
        if not encryptor.verify_checksum(decrypted_file, chunk_info['checksum_sha256']):
            raise ValueError(f"Checksum mismatch for chunk {chunk_num}")
        
        temp_files.append(decrypted_file)
        
        # Load to PostgreSQL
        print(f"   📥 Loading to PostgreSQL...")
        load_info = loader.load_parquet_to_table(
            decrypted_file,
            pg_config['schema'],
            pg_config['table']
        )
        
        total_loaded += load_info['rows_loaded']
        print(f"   ✅ Loaded {load_info['rows_loaded']:,} rows")
    
    # Verify total row count
    print(f"\n🔍 Verifying row count...")
    if loader.verify_row_count(pg_config['schema'], pg_config['table'], manifest['total_rows']):
        print(f"✅ Row count verified: {manifest['total_rows']:,} rows")
    else:
        print(f"⚠️  Row count mismatch - check logs")
    
    # Cleanup decrypted files
    if not keep_decrypted:
        print(f"\n🗑️  Cleaning up temporary files...")
        for temp_file in temp_files:
            temp_file.unlink()
        print(f"✅ Removed {len(temp_files)} temporary files")
    else:
        print(f"\n💾 Keeping {len(temp_files)} decrypted files")
    
    # Get table info
    print(f"\n📊 Table information:")
    table_info = loader.get_table_info(pg_config['schema'], pg_config['table'])
    
    print("\n" + "=" * 70)
    print("✅ IMPORT COMPLETE!")
    print("=" * 70)
    print(f"📊 Total: {total_loaded:,} rows loaded")
    print(f"📁 Table: {pg_config['schema']}.{pg_config['table']}")
    print(f"💾 Size: {table_info['table_size']}")
    print(f"💾 Encrypted files kept as backup in: {import_dir}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Import data from encrypted files to PostgreSQL"
    )
    parser.add_argument(
        "--table",
        help="Table name to import"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Import all configured tables"
    )
    parser.add_argument(
        "--password-file",
        help="Path to file containing decryption password"
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate table before loading"
    )
    parser.add_argument(
        "--keep-decrypted",
        action="store_true",
        help="Keep decrypted Parquet files (for debugging)"
    )
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        sys.exit(1)
    
    try:
        # Get settings
        settings = get_settings()
        import_base_dir = getattr(settings, 'import_base_dir', 'imports')
        
        # Get password (priority: --password-file > ENCRYPTION_PASSWORD env > prompt)
        env_password = getattr(settings, 'encryption_password', None)
        password = get_password(args.password_file, from_env=env_password)
        
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        # Import tables
        if args.table:
            # Import single table
            table_config = next(
                (t for t in config['tables'] if t['name'] == args.table),
                None
            )
            
            if not table_config:
                print(f"Error: Table '{args.table}' not found in config/tables.yaml")
                sys.exit(1)
            
            import_table(
                table_config,
                password,
                import_base_dir,
                truncate_first=args.truncate,
                keep_decrypted=args.keep_decrypted
            )
        else:
            # Import all tables
            print(f"\n{'=' * 70}")
            print(f"IMPORTING {len(config['tables'])} TABLES")
            print(f"{'=' * 70}")
            
            for table_config in config['tables']:
                try:
                    import_table(
                        table_config,
                        password,
                        import_base_dir,
                        truncate_first=args.truncate,
                        keep_decrypted=args.keep_decrypted
                    )
                except Exception as e:
                    logger.error(f"Failed to import {table_config['name']}: {e}")
                    print(f"\n❌ Failed to import {table_config['name']}: {e}")
            
            print(f"\n{'=' * 70}")
            print("ALL IMPORTS COMPLETE")
            print(f"{'=' * 70}")
        
    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        print(f"\n❌ Import failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
