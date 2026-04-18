#!/usr/bin/env python3
"""
Metadata Extraction Script
Run this script to extract metadata from Snowflake tables and generate PostgreSQL DDL
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.extractors.metadata_extractor import SnowflakeMetadataExtractor
from pipeline.transformers.obfuscator import MetadataObfuscator
from pipeline.config.settings import get_settings


def main():
    parser = argparse.ArgumentParser(description="Extract metadata from Snowflake tables")
    parser.add_argument("--table", help="Extract metadata for specific table")
    parser.add_argument("--all", action="store_true", help="Extract metadata for all configured tables")
    parser.add_argument("--no-check-changes", action="store_true", help="Disable metadata change detection (enabled by default)")
    parser.add_argument("--no-obfuscate", action="store_true", help="Disable name obfuscation (enabled by default)")
    
    args = parser.parse_args()
    
    # Change detection is enabled by default
    check_changes = not args.no_check_changes
    
    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        sys.exit(1)
    
    # Determine if obfuscation should be enabled
    settings = get_settings()
    obfuscate = not args.no_obfuscate and settings.obfuscate_names
    
    # Initialize obfuscator and get password if obfuscation enabled
    obfuscator = None
    password = None
    
    if obfuscate:
        print("Obfuscation: ENABLED")
        obfuscator = MetadataObfuscator()
        password = settings.encryption_password
    else:
        print("Obfuscation: DISABLED")
    
    print()
    
    # Initialize extractor with optional obfuscator
    extractor = SnowflakeMetadataExtractor(obfuscator=obfuscator)
    
    if args.table:
        # Extract single table
        print(f"Extracting metadata for table: {args.table}")
        
        # Load table configuration
        import yaml
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        # Find the table configuration
        table_config = next(
            (t for t in config['tables'] if t['name'] == args.table),
            None
        )
        
        if not table_config:
            print(f"Error: Table '{args.table}' not found in config/tables.yaml")
            sys.exit(1)
        
        sf_config = table_config['snowflake']
        pg_config = table_config['postgres']
        
        try:
            # Extract metadata from Snowflake.
            # Pass source_query so the actual output schema is used (e.g. for
            # tables that unpack VARIANT/JSON columns into typed columns).
            metadata = extractor.extract_table_metadata(
                sf_config['database'],
                sf_config['schema'],
                sf_config['table'],
                source_query=sf_config.get('source_query'),
            )

            # Safety net: ensure merge_keys exist in metadata even if the
            # describe step couldn't surface them. Case-insensitive match
            # because Snowflake uppercases unquoted identifiers.
            if sf_config.get('source_query'):
                existing_cols_ci = {col['name'].upper() for col in metadata['columns']}
                for key in table_config.get('merge_keys', []):
                    if key.upper() not in existing_cols_ci:
                        sf_type = extractor._infer_type_from_source_query(
                            sf_config['source_query'], key
                        )
                        pg_type = extractor._map_to_postgres_type(sf_type, None, None, None)
                        metadata['columns'].append({
                            'name': key,
                            'data_type': sf_type,
                            'is_nullable': True,
                            'default_value': None,
                            'max_length': None,
                            'precision': None,
                            'scale': None,
                            'position': len(metadata['columns']) + 1,
                            'postgres_type': pg_type,
                        })

            # Save metadata to file (with change checking enabled by default)
            metadata_file, comparison = extractor.save_metadata_to_file(
                metadata,
                args.table,
                check_changes=check_changes,
                password=password
            )
            
            # Display change alerts if checking changes
            if check_changes and comparison:
                if comparison.get("has_changes"):
                    print("\n" + "=" * 70)
                    print("⚠️  METADATA CHANGES DETECTED!")
                    print("=" * 70)
                    print(f"\nTable: {args.table}")
                    print(f"Summary: {comparison['summary']}")
                    
                    if comparison["changes"]:
                        print("\nDetailed Changes:")
                        for change in comparison["changes"]:
                            change_type = change["type"]
                            details = change.get("details", {})
                            if change_type == "column_added":
                                print(f"  + Column added: {change['column']} ({details.get('data_type', 'UNKNOWN')})")
                            elif change_type == "column_removed":
                                print(f"  - Column removed: {change['column']} ({details.get('data_type', 'UNKNOWN')})")
                            elif change_type == "column_type_changed":
                                print(f"  ~ Type changed: {change['column']}")
                                print(f"      {details.get('old_type')} → {details.get('new_type')}")
                            elif change_type == "column_nullable_changed":
                                nullable_str = "NULL" if details.get('new_nullable') else "NOT NULL"
                                print(f"  ~ Nullable changed: {change['column']} → {nullable_str}")
                            elif change_type == "column_position_changed":
                                print(f"  ~ Position changed: {change['column']}")
                                print(f"      Position {details.get('old_position')} → {details.get('new_position')}")
                    
                    print(f"\nArchived old metadata:")
                    print(f"  • metadata/encrypted/schemas/{args.table}_{datetime.now().strftime('%Y%m%d')}_metadata.json")
                    print(f"  • metadata/encrypted/ddl/{args.table}_{datetime.now().strftime('%Y%m%d')}_create.sql")
                    print("=" * 70)
                    print()
                else:
                    print("\n✓ No metadata changes detected\n")
            
            # Generate PostgreSQL DDL with indexes and unique-constraint for merge_keys.
            ddl = extractor.generate_postgres_ddl(
                metadata,
                pg_config['schema'],
                pg_config['table'],
                pg_config.get('indexes', []),
                merge_keys=table_config.get('merge_keys', []),
            )
            
            # Save DDL to file
            ddl_file = extractor.save_postgres_ddl(ddl, args.table, password=password)
            
            # Display results
            print("\nMetadata Extraction Results:")
            print("=" * 50)
            print(f"✓ {args.table}")
            print(f"  Columns: {len(metadata['columns'])}")
            print(f"  Rows: {metadata['statistics']['row_count']:,}")
            print(f"  Metadata: {metadata_file}")
            print(f"  DDL: {ddl_file}")
            print()
            
            # Display obfuscation status
            if obfuscate:
                print("=" * 50)
                print("Obfuscation Summary:")
                print(f"  • Metadata file encrypted with deterministic name")
                print(f"  • DDL file encrypted with deterministic name")
                print(f"  • File IDs are consistent across runs (same table = same ID)")
                print(f"  • Use same password to decrypt files")
                print()
            
        except Exception as e:
            print(f"\n✗ Failed to extract metadata for {args.table}")
            print(f"  Error: {e}")
            sys.exit(1)
    else:
        # Extract all tables
        print("Extracting metadata for all configured tables...")
        if check_changes:
            print("Change detection enabled - will alert on metadata changes\n")
        else:
            print("Change detection disabled\n")
        
        results = extractor.extract_all_configured_tables(
            check_changes=check_changes,
            password=password
        )
        
        # Display change alerts first
        if check_changes:
            changes_detected = False
            for table, result in results.items():
                if result["status"] == "success" and result.get("has_changes"):
                    if not changes_detected:
                        print("\n" + "=" * 70)
                        print("⚠️  METADATA CHANGES DETECTED!")
                        print("=" * 70)
                        changes_detected = True
                    
                    print(f"\nTable: {table}")
                    comparison = result["comparison"]
                    print(f"Summary: {comparison['summary']}")
                    
                    if comparison["changes"]:
                        print("\nDetailed Changes:")
                        for change in comparison["changes"]:
                            change_type = change["type"]
                            details = change.get("details", {})
                            if change_type == "column_added":
                                print(f"  + Column added: {change['column']} ({details.get('data_type', 'UNKNOWN')})")
                            elif change_type == "column_removed":
                                print(f"  - Column removed: {change['column']} ({details.get('data_type', 'UNKNOWN')})")
                            elif change_type == "column_type_changed":
                                print(f"  ~ Type changed: {change['column']}")
                                print(f"      {details.get('old_type')} → {details.get('new_type')}")
                            elif change_type == "column_nullable_changed":
                                nullable_str = "NULL" if details.get('new_nullable') else "NOT NULL"
                                print(f"  ~ Nullable changed: {change['column']} → {nullable_str}")
                            elif change_type == "column_position_changed":
                                print(f"  ~ Position changed: {change['column']}")
                                print(f"      Position {details.get('old_position')} → {details.get('new_position')}")
                    
                    print(f"\nArchived old metadata:")
                    print(f"  • metadata/encrypted/schemas/{table}_{datetime.now().strftime('%Y%m%d')}_metadata.json")
                    print(f"  • metadata/encrypted/ddl/{table}_{datetime.now().strftime('%Y%m%d')}_create.sql")
                    print()
            
            if changes_detected:
                print("=" * 70)
                print()
            elif any(r["status"] == "success" for r in results.values()):
                print("\n✓ No metadata changes detected for any tables\n")
        
        print("\nMetadata Extraction Results:")
        print("=" * 50)
        for table, result in results.items():
            if result["status"] == "success":
                status_icon = "✓"
                if check_changes:
                    if result.get("is_new"):
                        status_icon = "✓ [NEW]"
                    elif result.get("has_changes"):
                        status_icon = "✓ [CHANGED]"
                    else:
                        status_icon = "✓ [UNCHANGED]"
                
                print(f"{status_icon} {table}")
                print(f"  Columns: {result['columns']}")
                print(f"  Rows: {result['row_count']:,}")
                print(f"  Metadata: {result['metadata_file']}")
                print(f"  DDL: {result['ddl_file']}")
            else:
                print(f"✗ {table}")
                print(f"  Error: {result['error']}")
            print()
        
        # Display obfuscation status
        if obfuscate:
            print("=" * 50)
            print("Obfuscation Summary:")
            print(f"  • Metadata files encrypted with deterministic names")
            print(f"  • DDL files encrypted with deterministic names")
            print(f"  • File IDs are consistent across runs (same table = same ID)")
            print(f"  • Use same password to decrypt files")
            print()
        
if __name__ == "__main__":
    main()