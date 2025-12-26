#!/usr/bin/env python3
"""
Metadata Extraction Script
Run this script to extract metadata from Snowflake tables and generate PostgreSQL DDL
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.extractors.metadata_extractor import SnowflakeMetadataExtractor
from pipeline.loaders.postgres_loader import PostgreSQLLoader

def main():
    parser = argparse.ArgumentParser(description="Extract metadata from Snowflake tables")
    parser.add_argument("--table", help="Extract metadata for specific table")
    parser.add_argument("--all", action="store_true", help="Extract metadata for all configured tables")
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
        results = extractor.extract_all_configured_tables()
        
        print("\nMetadata Extraction Results:")
        print("=" * 50)
        for table, result in results.items():
            if result["status"] == "success":
                print(f"✓ {table}")
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