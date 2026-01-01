#!/usr/bin/env python3
"""
Compression Comparison Script
Compare different compression configurations side-by-side on real data

Usage:
    python scripts/compare_compression.py --table financial_data
    python scripts/compare_compression.py --table financial_data --chunk-size 50000
    python scripts/compare_compression.py --table financial_data --password-file ~/.encryption_key
"""
import sys
import json
import argparse
import getpass
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.extractors.data_extractor import SnowflakeDataExtractor
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.connections import SnowflakeConnectionManager
from pipeline.config.settings import get_settings
from pipeline.utils.logger import get_logger
import yaml

logger = get_logger(__name__)


def get_password(password_file: str = None, from_env: str = None) -> str:
    """Get encryption password from environment, file, or prompt"""
    if password_file:
        password_path = Path(password_file).expanduser()
        if password_path.exists():
            with open(password_path, 'r') as f:
                password = f.read().strip()
            logger.info(f"Password loaded from {password_file}")
            return password
    
    if from_env:
        logger.info("Using encryption password from .env")
        return from_env
    
    password = getpass.getpass("Enter encryption password: ")
    confirm = getpass.getpass("Confirm password: ")
    
    if password != confirm:
        raise ValueError("Passwords do not match!")
    
    return password


def export_with_config(
    table_config: dict,
    password: str,
    export_base_dir: str,
    conn_manager: SnowflakeConnectionManager,
    chunk_size: int,
    compression: str,
    compression_level: int,
    optimize_types: bool,
    config_name: str
) -> Dict[str, Any]:
    """
    Export table with specific compression configuration
    
    Returns:
        Dictionary with export statistics
    """
    table_name = table_config['name']
    sf_config = table_config['snowflake']
    
    print(f"\n{'=' * 70}")
    print(f"TESTING: {config_name}")
    print(f"{'=' * 70}")
    print(f"  Compression: {compression} level {compression_level}")
    print(f"  Type optimization: {'ENABLED' if optimize_types else 'DISABLED'}")
    print(f"  Chunk size: {chunk_size:,} rows")
    
    # Create export directory with config name
    export_dir = Path(export_base_dir) / f"{table_name}_comparison" / config_name.lower().replace(' ', '_')
    export_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Export directory: {export_dir}")
    
    # Initialize components
    extractor = SnowflakeDataExtractor(conn_manager)
    encryptor = FileEncryptor()
    
    # Build filter clause
    filter_config = sf_config.get('filter')
    filter_clause = extractor._build_filter_clause(filter_config)
    
    # Track statistics
    stats = {
        'config_name': config_name,
        'compression': compression,
        'compression_level': compression_level,
        'optimize_types': optimize_types,
        'chunk_size': chunk_size,
        'total_rows': 0,
        'total_chunks': 0,
        'total_compressed_size': 0,
        'total_encrypted_size': 0,
        'total_compression_time': 0,
        'total_encryption_time': 0,
        'type_optimization_stats': [],
        'chunks': []
    }
    
    # Extract and process chunks
    print(f"\n🔄 Extracting data...")
    
    chunk_num = 0
    start_time = time.time()
    
    for df_chunk in extractor.extract_table_chunks(
        sf_config['database'],
        sf_config['schema'],
        sf_config['table'],
        chunk_size=chunk_size,
        filter_clause=filter_clause
    ):
        chunk_num += 1
        stats['total_rows'] += len(df_chunk)
        
        # File names
        parquet_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet"
        encrypted_file = export_dir / f"data_chunk_{chunk_num:03d}.parquet.enc"
        
        print(f"\n  Chunk {chunk_num}: {len(df_chunk):,} rows")
        
        # Save to Parquet with compression
        comp_start = time.time()
        parquet_info = extractor.save_chunk_to_parquet(
            df_chunk,
            parquet_file,
            compression=compression,
            compression_level=compression_level,
            optimize_types=optimize_types
        )
        comp_time = time.time() - comp_start
        
        stats['total_compression_time'] += comp_time
        stats['total_compressed_size'] += parquet_info['size_bytes']
        
        print(f"    Compressed: {parquet_info['size_mb']:.2f} MB in {comp_time:.2f}s")
        
        # Track type optimization stats
        if 'type_optimization' in parquet_info:
            stats['type_optimization_stats'].append(parquet_info['type_optimization'])
        
        # Encrypt file
        enc_start = time.time()
        encryption_info = encryptor.encrypt_file(
            parquet_file,
            encrypted_file,
            password
        )
        enc_time = time.time() - enc_start
        
        stats['total_encryption_time'] += enc_time
        stats['total_encrypted_size'] += encryption_info['encrypted_size']
        
        print(f"    Encrypted: {encryption_info['encrypted_size'] / (1024*1024):.2f} MB in {enc_time:.2f}s")
        
        # Remove unencrypted file
        parquet_file.unlink()
        
        # Store chunk stats
        stats['chunks'].append({
            'chunk_number': chunk_num,
            'rows': len(df_chunk),
            'compressed_size': parquet_info['size_bytes'],
            'encrypted_size': encryption_info['encrypted_size'],
            'compression_time': comp_time,
            'encryption_time': enc_time
        })
    
    stats['total_chunks'] = chunk_num
    stats['total_time'] = time.time() - start_time
    
    # Calculate averages
    if stats['type_optimization_stats']:
        avg_reduction = sum(s['reduction_pct'] for s in stats['type_optimization_stats']) / len(stats['type_optimization_stats'])
        stats['avg_type_optimization_reduction'] = avg_reduction
    
    print(f"\n✅ {config_name} Complete!")
    print(f"  Total rows: {stats['total_rows']:,}")
    print(f"  Total chunks: {stats['total_chunks']}")
    print(f"  Compressed size: {stats['total_compressed_size'] / (1024*1024):.2f} MB")
    print(f"  Encrypted size: {stats['total_encrypted_size'] / (1024*1024):.2f} MB")
    print(f"  Total time: {stats['total_time']:.2f}s")
    if 'avg_type_optimization_reduction' in stats:
        print(f"  Avg type optimization: {stats['avg_type_optimization_reduction']:.1f}% reduction")
    
    return stats


def print_comparison_table(all_stats: List[Dict[str, Any]]):
    """Print comparison table of all configurations"""
    print("\n" + "=" * 100)
    print("COMPRESSION COMPARISON RESULTS")
    print("=" * 100)
    
    # Find baseline (first config)
    baseline = all_stats[0]
    baseline_size = baseline['total_encrypted_size']
    
    # Print header
    print(f"\n{'Configuration':<25} {'Size (MB)':<12} {'Savings':<12} {'Comp Time':<12} {'Total Time':<12} {'Type Opt'}")
    print("-" * 100)
    
    # Print each config
    for stats in all_stats:
        size_mb = stats['total_encrypted_size'] / (1024*1024)
        savings = (1 - stats['total_encrypted_size'] / baseline_size) * 100
        comp_time = stats['total_compression_time']
        total_time = stats['total_time']
        type_opt = f"{stats.get('avg_type_optimization_reduction', 0):.1f}%" if 'avg_type_optimization_reduction' in stats else "N/A"
        
        print(f"{stats['config_name']:<25} {size_mb:<12.2f} {savings:<12.1f}% {comp_time:<12.2f}s {total_time:<12.2f}s {type_opt}")
    
    # Print summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    
    best_compression = min(all_stats, key=lambda x: x['total_encrypted_size'])
    fastest = min(all_stats, key=lambda x: x['total_time'])
    
    print(f"\n🏆 Best Compression: {best_compression['config_name']}")
    print(f"   Size: {best_compression['total_encrypted_size'] / (1024*1024):.2f} MB")
    print(f"   Savings vs baseline: {(1 - best_compression['total_encrypted_size'] / baseline_size) * 100:.1f}%")
    
    print(f"\n⚡ Fastest: {fastest['config_name']}")
    print(f"   Time: {fastest['total_time']:.2f}s")
    
    print(f"\n💡 Recommendation:")
    if best_compression['config_name'] == fastest['config_name']:
        print(f"   Use '{best_compression['config_name']}' - best compression AND fastest!")
    else:
        time_diff = best_compression['total_time'] - fastest['total_time']
        size_diff = (1 - best_compression['total_encrypted_size'] / fastest['total_encrypted_size']) * 100
        print(f"   '{best_compression['config_name']}' saves {size_diff:.1f}% more space")
        print(f"   but takes {time_diff:.1f}s longer ({time_diff/fastest['total_time']*100:.1f}% slower)")
        print(f"   For huge datasets, the space savings are usually worth the extra time.")


def main():
    parser = argparse.ArgumentParser(
        description="Compare different compression configurations"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Table name to test"
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
        "--configs",
        nargs='+',
        choices=['baseline', 'level15', 'types', 'both', 'all'],
        default=['all'],
        help="Which configurations to test (default: all)"
    )
    
    args = parser.parse_args()
    
    try:
        # Get settings
        settings = get_settings()
        export_base_dir = getattr(settings, 'export_base_dir', 'exports')
        
        # Get password
        env_password = getattr(settings, 'encryption_password', None)
        password = get_password(args.password_file, from_env=env_password)
        
        # Load table configuration
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        table_config = next(
            (t for t in config['tables'] if t['name'] == args.table),
            None
        )
        
        if not table_config:
            print(f"Error: Table '{args.table}' not found in config/tables.yaml")
            sys.exit(1)
        
        # Define test configurations
        test_configs = []
        
        if 'all' in args.configs or 'baseline' in args.configs:
            test_configs.append({
                'name': 'Baseline (zstd-3)',
                'compression': 'zstd',
                'compression_level': 3,
                'optimize_types': False
            })
        
        if 'all' in args.configs or 'level15' in args.configs:
            test_configs.append({
                'name': 'Quick Win #1 (zstd-15)',
                'compression': 'zstd',
                'compression_level': 15,
                'optimize_types': False
            })
        
        if 'all' in args.configs or 'types' in args.configs:
            test_configs.append({
                'name': 'Type Optimization Only',
                'compression': 'zstd',
                'compression_level': 3,
                'optimize_types': True
            })
        
        if 'all' in args.configs or 'both' in args.configs:
            test_configs.append({
                'name': 'Quick Win #2 (zstd-15 + types)',
                'compression': 'zstd',
                'compression_level': 15,
                'optimize_types': True
            })
        
        print("\n" + "=" * 70)
        print(f"COMPRESSION COMPARISON TEST")
        print("=" * 70)
        print(f"Table: {args.table}")
        print(f"Configurations to test: {len(test_configs)}")
        print(f"Chunk size: {args.chunk_size:,} rows")
        
        # Connect to Snowflake once
        print("\n🔐 Connecting to Snowflake...")
        with SnowflakeConnectionManager() as conn_manager:
            print("✅ Connected to Snowflake")
            
            # Run tests
            all_stats = []
            
            for test_config in test_configs:
                try:
                    stats = export_with_config(
                        table_config,
                        password,
                        export_base_dir,
                        conn_manager,
                        args.chunk_size,
                        test_config['compression'],
                        test_config['compression_level'],
                        test_config['optimize_types'],
                        test_config['name']
                    )
                    all_stats.append(stats)
                    
                except Exception as e:
                    logger.error(f"Failed to test {test_config['name']}: {e}")
                    print(f"\n❌ Failed to test {test_config['name']}: {e}")
            
            # Print comparison
            if len(all_stats) > 1:
                print_comparison_table(all_stats)
            
            # Save detailed results
            results_file = Path(export_base_dir) / f"{args.table}_comparison" / "comparison_results.json"
            results_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(results_file, 'w') as f:
                json.dump({
                    'table': args.table,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'chunk_size': args.chunk_size,
                    'configurations': all_stats
                }, f, indent=2)
            
            print(f"\n📄 Detailed results saved to: {results_file}")
        
        print("\n✅ Comparison complete!")
        print(f"\n📁 Test files saved in: {export_base_dir}/{args.table}_comparison/")
        print("   You can delete this folder after reviewing the results.")
        
    except KeyboardInterrupt:
        print("\n\nComparison cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Comparison failed: {e}")
        print(f"\n❌ Comparison failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
