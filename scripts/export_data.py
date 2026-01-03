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
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.extractors.data_extractor import SnowflakeDataExtractor
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.transformers.obfuscator import DataObfuscator
from pipeline.connections import SnowflakeConnectionManager
from pipeline.config.settings import get_settings
from pipeline.utils.logger import get_logger
import yaml

logger = get_logger(__name__)


@dataclass
class ExportStatistics:
    """Statistics for an export operation."""
    total_chunks: int = 0
    chunks_new: int = 0          # Files that didn't exist before
    chunks_changed: int = 0      # Files that existed but content changed
    chunks_unchanged: int = 0    # Files that were skipped (identical)
    manifest_written: bool = False   # Whether manifest was written
    
    @property
    def chunks_written(self) -> int:
        """Total chunks written (new + changed)."""
        return self.chunks_new + self.chunks_changed
    
    @property
    def unchanged_percentage(self) -> float:
        """Percentage of chunks that were unchanged."""
        if self.total_chunks == 0:
            return 0.0
        return (self.chunks_unchanged / self.total_chunks) * 100


def get_password(password_file: str = None, from_env: str = None) -> str:
    """
    Get encryption password from environment, file, or prompt
    
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
        logger.info("Using encryption password from .env")
        return from_env
    
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
    conn_manager: SnowflakeConnectionManager,
    obfuscator: Optional[DataObfuscator] = None,
    chunk_size: int = 100000,
    compression: str = 'zstd',
    compression_level: int = 3,
    clean: bool = False
):
    """
    Export a single table
    
    Args:
        table_config: Table configuration from tables.yaml
        password: Encryption password
        export_base_dir: Base export directory
        conn_manager: Snowflake connection manager
        obfuscator: Optional DataObfuscator for name obfuscation
        chunk_size: Rows per chunk
        compression: Compression algorithm
        compression_level: Compression level
        clean: If True, delete existing export folder before starting
        
    Returns:
        Dictionary with export metadata including folder_id and manifest_file_id if obfuscated
    """
    table_name = table_config['name']
    sf_config = table_config['snowflake']
    
    # Check if obfuscation is enabled
    use_obfuscation = obfuscator is not None
    
    print("\n" + "=" * 70)
    print(f"EXPORTING TABLE: {table_name}")
    if use_obfuscation:
        print("🔒 Name obfuscation: ENABLED")
    print("=" * 70)
    
    # Generate folder name (obfuscated or original)
    if use_obfuscation:
        folder_name = obfuscator.generate_folder_id(table_name)
        print(f"📁 Export folder: {folder_name} (obfuscated)")
    else:
        folder_name = table_name
        print(f"📁 Export folder: {folder_name}")
    
    # Create export directory
    export_dir = Path(export_base_dir) / folder_name
    
    # Clean up existing folder if requested
    if clean and export_dir.exists():
        import shutil
        print(f"🧹 Cleaning up existing export folder...")
        print(f"   Deleting: {export_dir}")
        shutil.rmtree(export_dir)
        logger.info(f"Deleted existing export folder: {export_dir}")
        print(f"   ✅ Folder deleted")
    
    export_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Export directory: {export_dir}")
    
    # Initialize components with connection manager
    extractor = SnowflakeDataExtractor(conn_manager)
    encryptor = FileEncryptor()
    
    # Initialize content hash comparator for change detection
    from pipeline.utils.content_hash_comparator import ContentHashComparator
    comparator = ContentHashComparator(encryptor)
    
    # Initialize statistics tracking
    stats = ExportStatistics()
    
    # Build filter clause from configuration
    filter_config = sf_config.get('filter')
    filter_clause = extractor._build_filter_clause(filter_config)
    
    if filter_clause:
        print(f"\n🔍 Filter: {filter_clause}")
        logger.info(f"Using filter: {filter_clause}")
    else:
        print(f"\n📊 Extracting all data (no filter)")
    
    # Estimate table size (uses connection manager)
    print("\n🔄 Estimating table size...")
    size_info = extractor.estimate_table_size(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table'],
        filter_clause=filter_clause
    )
    
    if size_info.get('filtered'):
        print(f"✅ Filtered table size: {size_info['row_count']:,} rows ({size_info['size_mb']:.2f} MB estimated)")
    else:
        print(f"✅ Table size: {size_info['row_count']:,} rows ({size_info['size_mb']:.2f} MB)")
    
    # Display the full query that will be executed
    print(f"\n📋 SQL Query:")
    print("=" * 70)
    base_query = f"SELECT * FROM {sf_config['database']}.{sf_config['schema']}.{sf_config['table']}"
    if filter_clause:
        full_query = f"{base_query} {filter_clause}"
    else:
        full_query = base_query
    print(full_query)
    print("=" * 70)
    
    # Extract and process chunks (reuses connection)
    print(f"\n🔄 Extracting data in chunks of {chunk_size:,} rows...")
    
    chunks_metadata = []
    chunk_num = 0
    total_rows = 0
    file_mappings = {}  # Maps obfuscated file names to chunk info
    
    for df_chunk in extractor.extract_table_chunks(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table'],
        chunk_size=chunk_size,
        filter_clause=filter_clause
    ):
        chunk_num += 1
        total_rows += len(df_chunk)
        
        # Generate file name (obfuscated or original)
        if use_obfuscation:
            file_id = obfuscator.generate_file_id(chunk_num)
            parquet_file = export_dir / f"{file_id}.parquet"
            encrypted_file = export_dir / f"{file_id}.enc"
        else:
            parquet_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet"
            encrypted_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet.enc"
        
        print(f"\n📦 Processing chunk {chunk_num}...")
        print(f"   Rows: {len(df_chunk):,}")
        if use_obfuscation:
            print(f"   File: {encrypted_file.name} (obfuscated)")
        
        parquet_info = extractor.save_chunk_to_parquet(
            df_chunk,
            parquet_file,
            compression=compression,
            compression_level=compression_level
        )
        
        print(f"   Compressed: {parquet_info['size_mb']:.2f} MB")
        
        # Compute content hash for change detection
        content_hash = comparator.compute_file_hash(parquet_file)
        
        # Check if we need to write this file
        should_write, reason = comparator.should_write_file(
            content_hash,
            encrypted_file,
            password
        )
        
        # Update statistics
        stats.total_chunks += 1
        
        if should_write:
            # File needs to be written (new or changed)
            if reason == "new_file":
                stats.chunks_new += 1
                print(f"   📝 New file - encrypting...")
            elif reason == "content_changed":
                stats.chunks_changed += 1
                print(f"   🔄 Content changed - encrypting...")
            else:  # decryption_failed
                stats.chunks_changed += 1
                print(f"   ⚠️  Existing file corrupted - re-encrypting...")
            
            # Encrypt file
            encryption_info = encryptor.encrypt_file(
                parquet_file,
                encrypted_file,
                password
            )
            
            print(f"   Encrypted: {encryption_info['encrypted_size'] / (1024*1024):.2f} MB")
            
            # Store chunk metadata from new file
            chunk_metadata = {
                "chunk_number": chunk_num,
                "file": encrypted_file.name,
                "rows": len(df_chunk),
                "size_bytes": encryption_info['encrypted_size'],
                "checksum_sha256": encryption_info['checksum_sha256'],
                "encrypted": True
            }
        else:
            # File unchanged - skip write
            stats.chunks_unchanged += 1
            print(f"   ✅ Content unchanged - skipping write")
            
            # Read existing file metadata
            existing_size = encrypted_file.stat().st_size
            
            # Store chunk metadata from existing file
            chunk_metadata = {
                "chunk_number": chunk_num,
                "file": encrypted_file.name,
                "rows": len(df_chunk),
                "size_bytes": existing_size,
                "checksum_sha256": content_hash,  # Use content hash
                "encrypted": True
            }
        
        # Remove unencrypted Parquet file
        parquet_file.unlink()
        
        chunks_metadata.append(chunk_metadata)
        
        # Store file mapping if obfuscated
        if use_obfuscation:
            file_mappings[encrypted_file.name] = {
                "chunk_number": chunk_num,
                "rows": len(df_chunk)
            }
    
    # Create manifest
    manifest = {
        "table_name": table_name,
        "export_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_rows": total_rows,
        "total_chunks": chunk_num,
        "obfuscation_enabled": use_obfuscation,
        "snowflake_source": {
            "database": sf_config['database'],
            "schema": sf_config['schema'],
            "table": sf_config['table'],
            "filter": filter_clause if filter_clause else None
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
    
    # Add file mappings if obfuscated
    if use_obfuscation:
        manifest["file_mappings"] = file_mappings
    
    # Compute manifest content hash for change detection
    manifest_json = json.dumps(manifest, indent=2, sort_keys=True)
    manifest_hash = hashlib.sha256(manifest_json.encode('utf-8')).hexdigest()
    
    # Save manifest (encrypted if obfuscation enabled, plain if not)
    manifest_file_id = None
    manifest_needs_write = True
    
    if use_obfuscation:
        # Generate deterministic manifest file ID
        manifest_file_id = obfuscator.generate_manifest_id(table_name)
        manifest_file = export_dir / f"{manifest_file_id}.enc"
        
        # Check if manifest exists and compare
        if manifest_file.exists():
            existing_manifest_hash = comparator.decrypt_and_hash(manifest_file, password)
            if existing_manifest_hash:
                # Create temp file to compute hash of new manifest
                temp_manifest = export_dir / "manifest.json.tmp"
                with open(temp_manifest, 'w') as f:
                    f.write(manifest_json)
                
                new_manifest_hash = comparator.compute_file_hash(temp_manifest)
                temp_manifest.unlink()
                
                if new_manifest_hash == existing_manifest_hash:
                    manifest_needs_write = False
                    print(f"\n✅ Manifest unchanged - skipping write")
                    logger.info("Manifest content unchanged - skipping write")
        
        if manifest_needs_write:
            # Save as temporary JSON
            temp_manifest = export_dir / "manifest.json.tmp"
            with open(temp_manifest, 'w') as f:
                f.write(manifest_json)
            
            # Encrypt manifest
            print(f"\n🔐 Encrypting manifest as {manifest_file.name}...")
            encryptor.encrypt_file(temp_manifest, manifest_file, password)
            
            # Remove temporary file
            temp_manifest.unlink()
            
            logger.info(f"Manifest encrypted: {manifest_file}")
    else:
        # Plain JSON manifest (backward compatibility)
        manifest_file = export_dir / "manifest.json"
        
        # Check if manifest exists and compare
        if manifest_file.exists():
            try:
                existing_hash = comparator.compute_file_hash(manifest_file)
                # Create temp file to compute hash of new manifest
                temp_manifest = export_dir / "manifest.json.tmp"
                with open(temp_manifest, 'w') as f:
                    f.write(manifest_json)
                
                new_hash = comparator.compute_file_hash(temp_manifest)
                temp_manifest.unlink()
                
                if new_hash == existing_hash:
                    manifest_needs_write = False
                    print(f"\n✅ Manifest unchanged - skipping write")
                    logger.info("Manifest content unchanged - skipping write")
            except Exception as e:
                logger.warning(f"Failed to compare manifest: {e}")
                manifest_needs_write = True
        
        if manifest_needs_write:
            with open(manifest_file, 'w') as f:
                f.write(manifest_json)
            
            logger.info(f"Manifest saved: {manifest_file}")
    
    # Update statistics
    stats.manifest_written = manifest_needs_write
    
    print("\n" + "=" * 70)
    print("✅ EXPORT COMPLETE!")
    print("=" * 70)
    print(f"📁 Location: {export_dir}")
    if use_obfuscation:
        print(f"🔒 Folder ID: {folder_name}")
        print(f"🔒 Manifest ID: {manifest_file_id}")
    print(f"📊 Total: {total_rows:,} rows in {chunk_num} chunks")
    if filter_clause:
        print(f"🔍 Filter applied: {filter_clause}")
    
    # Display change detection statistics
    print(f"\n📈 Change Detection Statistics:")
    print(f"   Total chunks: {stats.total_chunks}")
    print(f"   New files: {stats.chunks_new}")
    print(f"   Changed files: {stats.chunks_changed}")
    print(f"   Unchanged files: {stats.chunks_unchanged}")
    print(f"   Files written: {stats.chunks_written}")
    print(f"   Unchanged: {stats.unchanged_percentage:.1f}%")
    if stats.manifest_written:
        print(f"   Manifest: Written")
    else:
        print(f"   Manifest: Unchanged (skipped)")
    
    total_size = sum(c['size_bytes'] for c in chunks_metadata)
    print(f"\n💾 Size: {total_size / (1024*1024):.2f} MB (encrypted)")
    print(f"🔐 Encryption: AES-256-GCM with PBKDF2 ({encryptor.iterations:,} iterations)")
    if use_obfuscation:
        print(f"🔒 Names: Obfuscated (all files encrypted)")
    print(f"⚠️  Remember your password - it's not stored anywhere!")
    print("=" * 70)
    
    # Return export metadata
    return {
        "table_name": table_name,
        "folder_id": folder_name if use_obfuscation else None,
        "manifest_file_id": manifest_file_id,
        "export_timestamp": manifest["export_timestamp"],
        "total_rows": total_rows,
        "total_chunks": chunk_num
    }


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
    parser.add_argument(
        "--no-obfuscate",
        action="store_true",
        help="Disable name obfuscation (obfuscation is enabled by default)"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing export folder before starting (useful for re-runs during testing)"
    )
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        sys.exit(1)
    
    try:
        # Get settings
        settings = get_settings()
        export_base_dir = getattr(settings, 'export_base_dir', 'exports')
        
        # Get compression settings from environment
        compression_type = getattr(settings, 'compression_type', 'zstd')
        compression_level = getattr(settings, 'compression_level', 3)
        
        # Determine if obfuscation should be enabled
        # Priority: --no-obfuscate flag > OBFUSCATE_NAMES env > default (True)
        if args.no_obfuscate:
            use_obfuscation = False
        else:
            use_obfuscation = getattr(settings, 'obfuscate_names', True)
        
        # Get password (priority: --password-file > ENCRYPTION_PASSWORD env > prompt)
        env_password = getattr(settings, 'encryption_password', None)
        password = get_password(args.password_file, from_env=env_password)
        
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        # Initialize obfuscator if needed
        obfuscator = None
        if use_obfuscation:
            obfuscator = DataObfuscator()
            print("\n🔒 Name obfuscation: ENABLED")
            print("   Folder and file names will use deterministic IDs")
            print("   Same table = same folder ID across runs")
        else:
            print("\n📁 Name obfuscation: DISABLED")
            print("   Using original table names for folders")
        
        # Create single Snowflake connection for all operations
        print("\n🔐 Connecting to Snowflake...")
        with SnowflakeConnectionManager() as conn_manager:
            print("✅ Connected to Snowflake (SSO authentication complete)")
            
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
                
                export_result = export_table(
                    table_config,
                    password,
                    export_base_dir,
                    conn_manager,
                    obfuscator=obfuscator,
                    chunk_size=args.chunk_size,
                    compression=compression_type,
                    compression_level=compression_level,
                    clean=args.clean
                )
            else:
                # Export all tables
                print(f"\n{'=' * 70}")
                print(f"EXPORTING {len(config['tables'])} TABLES")
                print(f"{'=' * 70}")
                
                for table_config in config['tables']:
                    try:
                        export_result = export_table(
                            table_config,
                            password,
                            export_base_dir,
                            conn_manager,
                            obfuscator=obfuscator,
                            chunk_size=args.chunk_size,
                            compression=compression_type,
                            compression_level=compression_level,
                            clean=args.clean
                        )
                    except Exception as e:
                        logger.error(f"Failed to export {table_config['name']}: {e}")
                        print(f"\n❌ Failed to export {table_config['name']}: {e}")
                
                print(f"\n{'=' * 70}")
                print("ALL EXPORTS COMPLETE")
                print(f"{'=' * 70}")
        
        # Connection automatically closed when exiting context manager
        print("\n✅ Snowflake connection closed")
        
        print("\n📋 Next steps:")
        print(f"1. Copy {export_base_dir}/ folder to PostgreSQL server")
        if use_obfuscation:
            print(f"   (Obfuscated folders use deterministic IDs - no index file needed)")
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
