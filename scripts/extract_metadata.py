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
from pipeline.loaders.postgres_loader import PostgreSQLLoader

def main():
    parser = argparse.ArgumentParser(description="Extract metadata from Snowflake tables")
    parser.add_argument("--table", help="Extract metadata for specific table")
    parser.add_argument("--all", action="store_true", help="Extract metadata for all configured tables")
    parser.add_argument("--check-changes", action="store_true", help="Check for metadata changes and alert if detected")
    parser.add_argument("--force", action="store_true", help="Force re-extraction even if no changes detected")
    parser.add_argument("--create-postgres", action="store_true", help="Also create PostgreSQL tables")
    parser.add_argument("--drop-existing", action="store_true", help="Drop existing PostgreSQL tables")
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        sys.exit(1)
    
    extractor = SnowflakeMetadataExtractor()
    
    if args.table:
        # Extract single table
        print(f"Extracting metadata for table: {args.table}")
        # Implementation for single table would go here
        print("Single table extraction not implemented yet - use --all for now")
    else:
        # Extract all tables
        print("Extracting metadata for all configured tables...")
        if args.check_changes:
            print("Change detection enabled - will alert on metadata changes\n")
        
        results = extractor.extract_all_configured_tables(
            check_changes=args.check_changes,
            force=args.force
        )
        
        # Display change alerts first
        if args.check_changes:
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
                            if change_type == "column_added":
                                print(f"  + Column added: {change['column']} ({change['data_type']})")
                            elif change_type == "column_removed":
                                print(f"  - Column removed: {change['column']} ({change['data_type']})")
                            elif change_type == "type_changed":
                                print(f"  ~ Type changed: {change['column']}")
                                print(f"      {change['old_type']} → {change['new_type']}")
                            elif change_type == "nullable_changed":
                                nullable_str = "NULL" if change['new_nullable'] else "NOT NULL"
                                print(f"  ~ Nullable changed: {change['column']} → {nullable_str}")
                            elif change_type == "position_changed":
                                print(f"  ~ Position changed: {change['column']}")
                                print(f"      Position {change['old_position']} → {change['new_position']}")
                    
                    print(f"\nArchived old metadata:")
                    print(f"  • metadata/schemas/{table}_{datetime.now().strftime('%Y%m%d')}_metadata.json")
                    print(f"  • metadata/ddl/{table}_{datetime.now().strftime('%Y%m%d')}_create.sql")
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
                if args.check_changes:
                    if result.get("is_new"):
                        status_icon = "✓ [NEW]"
                    elif result.get("has_changes"):
                        status_icon = "✓ [CHANGED]"
                    elif not args.force:
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
        
        # Create PostgreSQL tables if requested
        if args.create_postgres:
            print("Creating PostgreSQL tables...")
            loader = PostgreSQLLoader()
            pg_results = loader.create_all_configured_tables(drop_if_exists=args.drop_existing)
            
            print("\nPostgreSQL Table Creation Results:")
            print("=" * 50)
            for table, result in pg_results.items():
                if result["status"] == "success":
                    print(f"✓ {table}")
                    print(f"  Schema: {result['schema']}")
                    print(f"  Table: {result['table']}")
                    print(f"  Columns: {result['columns']}")
                    if result.get("verification"):
                        verification = result["verification"]
                        status = "✓" if verification["matches"] else "✗"
                        print(f"  Verification: {status}")
                        if verification["differences"]:
                            for diff in verification["differences"]:
                                print(f"    - {diff}")
                else:
                    print(f"✗ {table}")
                    print(f"  Error: {result['error']}")
                print()

if __name__ == "__main__":
    main()