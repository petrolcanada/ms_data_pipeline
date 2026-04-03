#!/usr/bin/env python3
"""
Data Export Script - Phase 1
Extract data from Snowflake, compress, encrypt, and save for manual transfer

Usage:
    python scripts/export_data.py --table financial_data
    python scripts/export_data.py --all
    python scripts/export_data.py --all --archive
    python scripts/export_data.py --all --repo-mode seed-delta --bundle
"""
import sys
import json
import argparse
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.extractors.data_extractor import SnowflakeDataExtractor
from pipeline.extractors.metadata_extractor import SnowflakeMetadataExtractor
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.transformers.obfuscator import DataObfuscator, MetadataObfuscator
from pipeline.connections import SnowflakeConnectionManager
from pipeline.state.watermark_manager import WatermarkManager
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


def export_table(
    table_config: dict,
    password: str,
    export_base_dir: str,
    conn_manager: SnowflakeConnectionManager,
    obfuscator: Optional[DataObfuscator] = None,
    chunk_size: int = 100000,
    compression: str = 'zstd',
    compression_level: int = 9,
    sort_before_compress: bool = True,
    use_dictionary_encoding: bool = True,
    full_reload: bool = False,
    table_index: int = 0,
    total_tables: int = 0,
):
    """
    Export a single table.
    
    Returns:
        Dictionary with export metadata including folder_id and manifest_file_id if obfuscated
    """
    table_name = table_config['name']
    sf_config = table_config['snowflake']
    sync_mode = table_config.get('sync_mode', 'full')
    watermark_column = table_config.get('watermark_column')
    merge_keys = table_config.get('merge_keys', [])
    
    sort_columns: Optional[List[str]] = None
    if sort_before_compress:
        sort_cols = list(merge_keys)
        if watermark_column and watermark_column not in sort_cols:
            sort_cols.append(watermark_column)
        sort_columns = sort_cols if sort_cols else None
    
    use_obfuscation = obfuscator is not None
    
    # Compact header
    progress = f"[{table_index}/{total_tables}]" if total_tables else ""
    print(f"\n  {progress}  {table_name}")
    
    folder_name = obfuscator.generate_folder_id(table_name) if use_obfuscation else table_name
    
    export_dir = Path(export_base_dir) / "data" / "encrypted" / folder_name
    export_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Export directory: {export_dir}")
    
    extractor = SnowflakeDataExtractor(conn_manager)
    encryptor = FileEncryptor()
    
    from pipeline.utils.content_hash_comparator import ContentHashComparator
    comparator = ContentHashComparator(encryptor)
    
    stats = ExportStatistics()
    
    # Build filter and watermark
    filter_config = sf_config.get('filter')
    filter_clause = extractor._build_filter_clause(filter_config)
    
    watermark_mgr = WatermarkManager(state_dir=get_settings().state_dir)
    watermark_value = None
    if sync_mode in ("incremental", "upsert") and watermark_column:
        if full_reload:
            logger.info(f"Full reload: ignoring watermark for {table_name}")
        else:
            watermark_value = watermark_mgr.get_watermark(table_name)
            if watermark_value:
                filter_clause = extractor.inject_watermark(filter_clause, watermark_column, watermark_value)
    
    # Build the full query (used in manifest and logging)
    base_query = f"SELECT * FROM {sf_config['database']}.{sf_config['schema']}.{sf_config['table']}"
    full_query = f"{base_query} {filter_clause}" if filter_clause else base_query
    logger.info(f"Query: {full_query}")
    
    # Estimate table size
    size_info = extractor.estimate_table_size(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table'],
        filter_clause=filter_clause
    )
    estimated_rows = size_info['row_count']
    estimated_mb = size_info['size_mb']
    
    # Print mode info on one line
    mode_parts = [sync_mode]
    if full_reload:
        mode_parts.append("full-reload")
    elif watermark_value:
        mode_parts.append(f"from {watermark_value}")
    print(f"          {estimated_rows:,} rows est. | {', '.join(mode_parts)}")
    
    chunks_metadata = []
    chunk_num = 0
    total_rows = 0
    max_watermark_value = None
    
    for df_chunk in extractor.extract_table_chunks(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table'],
        chunk_size=chunk_size,
        filter_clause=filter_clause
    ):
        chunk_num += 1
        total_rows += len(df_chunk)
        
        if watermark_column and watermark_column in df_chunk.columns:
            chunk_max = df_chunk[watermark_column].dropna().max()
            if chunk_max is not None:
                chunk_max_str = str(chunk_max)
                if max_watermark_value is None or chunk_max_str > max_watermark_value:
                    max_watermark_value = chunk_max_str
        
        if use_obfuscation:
            file_id = obfuscator.generate_chunk_file_id(table_name, chunk_num)
            parquet_file = export_dir / f"{file_id}.parquet"
            encrypted_file = export_dir / f"{file_id}.enc"
        else:
            parquet_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet"
            encrypted_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet.enc"
        
        parquet_info = extractor.save_chunk_to_parquet(
            df_chunk,
            parquet_file,
            compression=compression,
            compression_level=compression_level,
            sort_columns=sort_columns,
            use_dictionary_encoding=use_dictionary_encoding,
        )
        
        content_hash = comparator.compute_file_hash(parquet_file)
        should_write, reason = comparator.should_write_file(
            content_hash, encrypted_file, password
        )
        
        stats.total_chunks += 1
        
        if should_write:
            if reason == "new_file":
                stats.chunks_new += 1
            else:
                stats.chunks_changed += 1
            
            encryption_info = encryptor.encrypt_file(
                parquet_file, encrypted_file, password
            )
            
            chunk_metadata = {
                "chunk_number": chunk_num,
                "file": encrypted_file.name,
                "rows": len(df_chunk),
                "size_bytes": encryption_info['encrypted_size'],
                "checksum_sha256": encryption_info['checksum_sha256'],
                "encrypted": True
            }
        else:
            stats.chunks_unchanged += 1
            existing_size = encrypted_file.stat().st_size
            chunk_metadata = {
                "chunk_number": chunk_num,
                "file": encrypted_file.name,
                "rows": len(df_chunk),
                "size_bytes": existing_size,
                "checksum_sha256": content_hash,
                "encrypted": True
            }
        
        parquet_file.unlink()
        chunks_metadata.append(chunk_metadata)
    
    # Build manifest with full debugging context
    total_size = sum(c['size_bytes'] for c in chunks_metadata)
    manifest = {
        "table_name": table_name,
        "export_timestamp": datetime.now().astimezone().isoformat(),
        "sync_mode": sync_mode,
        "full_reload": full_reload,
        "total_rows": total_rows,
        "total_chunks": chunk_num,
        "total_size_bytes": total_size,
        "obfuscation_enabled": use_obfuscation,
        "query": full_query,
        "snowflake_source": {
            "database": sf_config['database'],
            "schema": sf_config['schema'],
            "table": sf_config['table'],
            "filter": filter_clause if filter_clause else None
        },
        "watermark": {
            "column": watermark_column,
            "value_used": watermark_value,
            "max_exported": max_watermark_value,
        },
        "merge_keys": merge_keys,
        "estimate": {
            "row_count": estimated_rows,
            "size_mb": estimated_mb,
        },
        "encryption": {
            "algorithm": "AES-256-GCM",
            "key_derivation": "PBKDF2-HMAC-SHA256",
            "iterations": encryptor.iterations,
        },
        "compression": {
            "algorithm": compression,
            "level": compression_level
        },
        "change_detection": {
            "new": stats.chunks_new,
            "changed": stats.chunks_changed,
            "unchanged": stats.chunks_unchanged,
        },
        "chunks": chunks_metadata
    }
    
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
    
    # Write plain manifest copy to data/raw/ for debugging
    raw_dir = Path(export_base_dir) / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_manifest_file = raw_dir / f"{table_name}_manifest.json"
    with open(raw_manifest_file, 'w') as f:
        f.write(manifest_json)
    logger.info(f"Raw manifest saved: {raw_manifest_file}")
    
    # Compact result line
    change_parts = []
    if stats.chunks_new:
        change_parts.append(f"{stats.chunks_new} new")
    if stats.chunks_changed:
        change_parts.append(f"{stats.chunks_changed} changed")
    if stats.chunks_unchanged:
        change_parts.append(f"{stats.chunks_unchanged} unchanged")
    change_summary = ", ".join(change_parts) if change_parts else "no chunks"
    
    print(f"          {total_rows:,} rows -> {chunk_num} chunk(s) ({total_size / (1024*1024):.2f} MB) | {change_summary}")
    
    # Update watermark
    if sync_mode in ("incremental", "upsert") and watermark_column and total_rows > 0:
        effective_watermark = max_watermark_value or manifest["export_timestamp"]
        watermark_mgr.update_watermark(
            table_name,
            effective_watermark,
            rows_exported=total_rows,
            export_timestamp=manifest["export_timestamp"],
        )
        logger.info(f"Watermark updated for {table_name}: {effective_watermark}")
    
    return {
        "table_name": table_name,
        "folder_id": folder_name if use_obfuscation else None,
        "manifest_file_id": manifest_file_id,
        "export_timestamp": manifest["export_timestamp"],
        "total_rows": total_rows,
        "total_chunks": chunk_num,
        "total_size_bytes": total_size,
        "sync_mode": sync_mode,
        "stats": stats,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Export data from Snowflake for offline transfer"
    )
    parser.add_argument(
        "--table",
        nargs="+",
        help="One or more table names to export"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all configured tables"
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
    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Ignore watermark state and extract all rows, but still update the watermark after export"
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip metadata extraction (only export data)"
    )
    # --- Compression optimization flags ---
    parser.add_argument(
        "--no-sort",
        action="store_true",
        help="Disable pre-sort optimization (sorting is enabled by default for better compression)"
    )
    parser.add_argument(
        "--no-dictionary",
        action="store_true",
        help="Disable automatic dictionary encoding for low-cardinality columns"
    )
    # --- Delivery flags ---
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Create a tar.gz archive per table for single-file transport"
    )
    parser.add_argument(
        "--repo-mode",
        choices=["single", "repo"],
        default=None,
        help="Git delivery: 'single' (no git) or 'repo' (disposable dataset repo, reset each run)"
    )
    parser.add_argument(
        "--bundle",
        action="store_true",
        help="Create a git bundle for air-gapped transfer after committing"
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Push dataset repo to its remote after committing (requires DATASET_REPO_URL in .env)"
    )
    parser.add_argument(
        "--purpose",
        help="Custom run purpose / description embedded in the delivery commit and manifest"
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
        compression_level = getattr(settings, 'compression_level', 9)
        
        # Compression optimization
        sort_before_compress = settings.sort_before_compress and not args.no_sort
        use_dict_encoding = settings.use_dictionary_encoding and not args.no_dictionary
        
        # Repo mode: CLI flag > env > default
        repo_mode = args.repo_mode or settings.repo_mode
        
        # Determine if obfuscation should be enabled
        if args.no_obfuscate:
            use_obfuscation = False
        else:
            use_obfuscation = getattr(settings, 'obfuscate_names', True)
        
        password = settings.encryption_password
        
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        obfuscator = DataObfuscator() if use_obfuscation else None
        
        # Initialize repo manager if needed
        repo_manager = None
        if repo_mode == "repo":
            from pipeline.utils.repo_manager import DatasetRepoManager
            repo_manager = DatasetRepoManager(
                repo_dir=settings.dataset_repo_dir,
                remote_url=settings.dataset_repo_url,
            )
        
        # Wipe the data/ folder for a fresh delivery
        data_dir = Path(export_base_dir) / "data"
        if data_dir.exists():
            from pipeline.utils.repo_manager import _force_remove_readonly
            shutil.rmtree(str(data_dir), onerror=_force_remove_readonly)
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "encrypted").mkdir(exist_ok=True)
        (data_dir / "raw").mkdir(exist_ok=True)
        
        # Determine tables to export
        if args.table:
            table_configs = [
                t for t in config['tables'] if t['name'] in args.table
            ]
            not_found = set(args.table) - {t['name'] for t in table_configs}
            if not_found:
                print(f"Error: Table(s) not found in config/tables.yaml: {', '.join(not_found)}")
                sys.exit(1)
        else:
            table_configs = config['tables']
        
        # Print compact header
        flags = [f"{compression_type}-{compression_level}"]
        if use_obfuscation:
            flags.append("obfuscated")
        if args.full_reload:
            flags.append("full-reload")
        if repo_mode == "repo":
            flags.append("repo")
        print(f"\n{'=' * 70}")
        print(f"EXPORT  {len(table_configs)} table(s) | {' | '.join(flags)}")
        print(f"  Data: {data_dir}")
        print(f"{'=' * 70}")
        
        with SnowflakeConnectionManager() as conn_manager:
            # --- Metadata extraction (reuses the same SSO connection) ---
            if not args.skip_metadata:
                meta_obfuscator = MetadataObfuscator() if use_obfuscation else None
                meta_extractor = SnowflakeMetadataExtractor(obfuscator=meta_obfuscator)
                
                print(f"\n  Metadata: extracting {len(table_configs)} table(s)...")
                meta_results = meta_extractor.extract_all_configured_tables(
                    check_changes=True,
                    password=password,
                    conn=conn_manager.get_connection(),
                )
                
                meta_ok = sum(1 for r in meta_results.values() if r["status"] == "success")
                meta_changed = sum(1 for r in meta_results.values() if r.get("has_changes"))
                meta_new = sum(1 for r in meta_results.values() if r.get("is_new"))
                meta_failed = sum(1 for r in meta_results.values() if r["status"] != "success")
                
                parts = [f"{meta_ok} refreshed"]
                if meta_new:
                    parts.append(f"{meta_new} new")
                if meta_changed:
                    parts.append(f"{meta_changed} changed")
                if meta_failed:
                    parts.append(f"{meta_failed} failed")
                print(f"  Metadata: {', '.join(parts)}")
            
            export_results = []
            
            for i, table_config in enumerate(table_configs, 1):
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
                        sort_before_compress=sort_before_compress,
                        use_dictionary_encoding=use_dict_encoding,
                        full_reload=args.full_reload,
                        table_index=i,
                        total_tables=len(table_configs),
                    )
                    export_results.append(export_result)
                except Exception as e:
                    logger.error(f"Failed to export {table_config['name']}: {e}")
                    print(f"\n  [!] FAILED: {table_config['name']}: {e}")
        
        # Summary
        if export_results:
            total_rows = sum(r['total_rows'] for r in export_results)
            total_bytes = sum(r.get('total_size_bytes', 0) for r in export_results)
            total_new = sum(r['stats'].chunks_new for r in export_results)
            total_changed = sum(r['stats'].chunks_changed for r in export_results)
            total_unchanged = sum(r['stats'].chunks_unchanged for r in export_results)
            
            print(f"\n{'=' * 70}")
            print(f"DONE  {len(export_results)} table(s) | {total_rows:,} rows | {total_bytes / (1024*1024):.2f} MB")
            print(f"  New: {total_new} | Changed: {total_changed} | Unchanged: {total_unchanged}")
            print(f"{'=' * 70}")
        
        # --- Post-export: archive / repo / bundle ---
        
        if args.archive:
            from pipeline.utils.archive import create_table_archive
            print(f"\n  Archiving...")
            for result in export_results:
                folder = result.get("folder_id") or result["table_name"]
                archive_info = create_table_archive(
                    Path(export_base_dir) / "data" / "encrypted", folder, label=result["table_name"],
                )
                print(f"    {result['table_name']}: {archive_info['size_mb']:.2f} MB")
        
        if repo_manager and export_results:
            repo_manager.reset()
            for result in export_results:
                folder = result.get("folder_id") or result["table_name"]
                source = Path(export_base_dir) / "data" / "encrypted" / folder
                if source.is_dir():
                    repo_manager.stage_table(source, folder)
            
            from pipeline.utils.repo_manager import build_delivery_manifest
            delivery = build_delivery_manifest(
                export_results, table_configs,
                run_purpose=args.purpose,
            )
            repo_manager.write_delivery_manifest(
                delivery,
                password=password if use_obfuscation else None,
            )
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            commit_msg = f"{delivery['run_purpose']} [{timestamp}]"
            sha = repo_manager.commit(commit_msg)
            print(f"\n  Repo: committed {sha[:8] if sha else '(no changes)'}")
        
        if args.push and repo_manager:
            if settings.dataset_repo_url:
                push_info = repo_manager.push()
                print(f"  Pushed: {push_info['branch']} @ {push_info['head']}")
            else:
                print("  [!] No DATASET_REPO_URL configured, skipping push")
        elif args.push and not repo_manager:
            print("  [!] --push requires --repo-mode repo")
        
        if args.bundle and repo_manager:
            bundle_dir = Path(settings.bundle_output_dir)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            bundle_info = repo_manager.create_bundle(
                bundle_dir / f"dataset_{ts}.bundle",
            )
            print(f"  Bundle: {bundle_info['size_mb']:.2f} MB -> {bundle_info['bundle_path']}")
        elif args.bundle and not repo_manager:
            print("  [!] --bundle requires --repo-mode repo")
        
    except KeyboardInterrupt:
        print("\n\nExport cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Export failed: {e}")
        print(f"\n  Export failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
