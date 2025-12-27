#!/usr/bin/env python3
"""
Data Export Script - Phase 1
Extract data from Snowflake, compress, encrypt, and save for manual transfer

Usage:
    python scripts/export_data.py --table financial_data
    python scripts/export_data.py --table financial_data --password-file ~/.encryption_key
    python scripts/export_data.py --all
"""
import sys
import json
import argparse
import getpass
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.extractors.data_extractor import SnowflakeDataExtractor
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.config.settings import get_settings
from pipeline.utils.logger import get_logger
import yaml

logger = get_logger(__name__)


def get_password(password_file: str = None) -> str:
    """Get encryption password from file or prompt"""
    if password_file:
        password_path = Path(password_file).expanduser()
        if password_path.exists():
            with open(password_path, 'r') as f:
                password = f.read().strip()
            logger.info(f"Password loaded from {password_file}")
            return password
        else:
            logger.warning(f"Password file not found: {password_file}")
    
    # Prompt for password
    password = getpass.getpass("Enter encryption password: ")
    confirm = getpass.getpass("Confirm password: ")
    
    if password != confirm:
        raise ValueError("Passwords do not match!")
    
    return password


def export_table(
    table_config: dict,
    password: str,
    export_base_dir: str,
    chunk_size: int = 100000,
    compression: str = 'zstd',
    compression_level: int = 3
):
    """
    Export a single table
    
    Args:
        table_config: Table configuration from tables.yaml
        password: Encryption password
        export_base_dir: Base export directory
        chunk_size: Rows per chunk
        compression: Compression algorithm
        compression_level: Compression level
    """
    table_name = table_config['name']
    sf_config = table_config['snowflake']
    
    print("\n" + "=" * 70)
    print(f"EXPORTING TABLE: {table_name}")
    print("=" * 70)
    
    # Create export directory
    export_dir = Path(export_base_dir) / table_name
    export_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Export directory: {export_dir}")
    
    # Initialize components
    extractor = SnowflakeDataExtractor()
    encryptor = FileEncryptor()
    
    # Estimate table size
    print("\n🔄 Estimating table size...")
    size_info = extractor.estimate_table_size(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table']
    )
    
    print(f"✅ Table size: {size_info['row_count']:,} rows ({size_info['size_mb']:.2f} MB)")
    
    # Extract and process chunks
    print(f"\n🔄 Extracting data in chunks of {chunk_size:,} rows...")
    
    chunks_metadata = []
    chunk_num = 0
    total_rows = 0
    
    for df_chunk in extractor.extract_table_chunks(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table'],
        chunk_size=chunk_size
    ):
        chunk_num += 1
        total_rows += len(df_chunk)
        
        # Save as Parquet
        parquet_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet"
        print(f"\n📦 Processing chunk {chunk_num}...")
        print(f"   Rows: {len(df_chunk):,}")
        
        parquet_info = extractor.save_chunk_to_parquet(
            df_chunk,
            parquet_file,
            compression=compression,
            compression_level=compression_level
        )
        
        print(f"   Compressed: {parquet_info['size_mb']:.2f} MB")
        
        # Encrypt file
        encrypted_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet.enc"
        print(f"   🔐 Encrypting...")
        
        encryption_info = encryptor.encrypt_file(
            parquet_file,
            encrypted_file,
            password
        )
        
        print(f"   Encrypted: {encryption_info['encrypted_size'] / (1024*1024):.2f} MB")
        
        # Remove unencrypted Parquet file
        parquet_file.unlink()
        
        # Store chunk metadata
        chunks_metadata.append({
            "chunk_number": chunk_num,
            "file": encrypted_file.name,
            "rows": len(df_chunk),
            "size_bytes": encryption_info['encrypted_size'],
            "checksum_sha256": encryption_info['checksum_sha256'],
            "encrypted": True
        })
    
    # Create manifest
    manifest = {
        "table_name": table_name,
        "export_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_rows": total_rows,
        "total_chunks": chunk_num,
        "snowflake_source": {
            "database": sf_config['database'],
            "schema": sf_config['schema'],
            "table": sf_config['table']
        },
        "encryption": {
            "algorithm": "AES-256-GCM",
            "key_derivation": "PBKDF2-HMAC-SHA256",
            "iterations": encryptor.iterations,
            "salt": chunks_metadata[0]['checksum_sha256'][:32] if chunks_metadata else None
        },
        "compression": {
            "algorithm": compression,
            "level": compression_level
        },
        "chunks": chunks_metadata
    }
    
    manifest_file = export_dir / "manifest.json"
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print("\n" + "=" * 70)
    print("✅ EXPORT COMPLETE!")
    print("=" * 70)
    print(f"📁 Location: {export_dir}")
    print(f"📊 Total: {total_rows:,} rows in {chunk_num} chunks")
    
    total_size = sum(c['size_bytes'] for c in chunks_metadata)
    print(f"💾 Size: {total_size / (1024*1024):.2f} MB (encrypted)")
    print(f"🔐 Encryption: AES-256-GCM with PBKDF2 ({encryptor.iterations:,} iterations)")
    print(f"⚠️  Remember your password - it's not stored anywhere!")
    print("=" * 70)
    
    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Export data from Snowflake for offline transfer"
    )
    parser.add_argument(
        "--table",
        help="Table name to export"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all configured tables"
    )
    parser.add_argument(
        "--password-file",
        help="Path to file containing encryption password"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Rows per chunk (default: 100000)"
    )
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        sys.exit(1)
    
    try:
        # Get settings
        settings = get_settings()
        export_base_dir = getattr(settings, 'export_base_dir', 'exports')
        
        # Get password
        password = get_password(args.password_file)
        
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        # Export tables
        if args.table:
            # Export single table
            table_config = next(
                (t for t in config['tables'] if t['name'] == args.table),
                None
            )
            
            if not table_config:
                print(f"Error: Table '{args.table}' not found in config/tables.yaml")
                sys.exit(1)
            
            export_table(
                table_config,
                password,
                export_base_dir,
                chunk_size=args.chunk_size
            )
        else:
            # Export all tables
            print(f"\n{'=' * 70}")
            print(f"EXPORTING {len(config['tables'])} TABLES")
            print(f"{'=' * 70}")
            
            for table_config in config['tables']:
                try:
                    export_table(
                        table_config,
                        password,
                        export_base_dir,
                        chunk_size=args.chunk_size
                    )
                except Exception as e:
                    logger.error(f"Failed to export {table_config['name']}: {e}")
                    print(f"\n❌ Failed to export {table_config['name']}: {e}")
            
            print(f"\n{'=' * 70}")
            print("ALL EXPORTS COMPLETE")
            print(f"{'=' * 70}")
        
        print("\n📋 Next steps:")
        print(f"1. Copy {export_base_dir}/ folder to PostgreSQL server")
        print(f"2. Run: python scripts/import_data.py --table {args.table or '<table_name>'}")
        
    except KeyboardInterrupt:
        print("\n\nExport cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Export failed: {e}")
        print(f"\n❌ Export failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
