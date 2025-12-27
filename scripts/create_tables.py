#!/usr/bin/env python3
"""
Create Tables Script
Loops through DDL files and creates tables in PostgreSQL if they don't exist

This script should be run on the PostgreSQL server after metadata files are transferred.

Usage:
    python scripts/create_tables.py --all
    python scripts/create_tables.py --table financial_data
    python scripts/create_tables.py --all --drop-existing
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.loaders.postgres_loader import PostgreSQLLoader
from pipeline.utils.logger import get_logger
import yaml

logger = get_logger(__name__)


def create_table(table_name: str, drop_if_exists: bool = False):
    """
    Create a single table from DDL
    
    Args:
        table_name: Name of the table
        drop_if_exists: Drop table if it already exists
    """
    print(f"\n{'=' * 70}")
    print(f"Creating Table: {table_name}")
    print(f"{'=' * 70}")
    
    loader = PostgreSQLLoader()
    
    try:
        # Check if metadata and DDL files exist
        metadata_file = Path(f"metadata/schemas/{table_name}_metadata.json")
        ddl_file = Path(f"metadata/ddl/{table_name}_create.sql")
        
        if not metadata_file.exists():
            print(f"❌ Metadata file not found: {metadata_file}")
            print(f"   Make sure metadata files are transferred to this server")
            return False
        
        if not ddl_file.exists():
            print(f"❌ DDL file not found: {ddl_file}")
            print(f"   Make sure DDL files are transferred to this server")
            return False
        
        print(f"✅ Found metadata: {metadata_file.name}")
        print(f"✅ Found DDL: {ddl_file.name}")
        
        # Create table
        print(f"\n🔄 Creating table...")
        result = loader.create_table_from_metadata(table_name, drop_if_exists=drop_if_exists)
        
        print(f"\n✅ Table created successfully!")
        print(f"   Schema: {result['schema']}")
        print(f"   Table: {result['table']}")
        print(f"   Columns: {result['columns']}")
        
        # Verify table structure
        print(f"\n🔍 Verifying table structure...")
        verification = loader.verify_table_structure(table_name)
        
        if verification['matches']:
            print(f"✅ Table structure verified!")
            print(f"   Snowflake columns: {verification['snowflake_columns']}")
            print(f"   PostgreSQL columns: {verification['postgres_columns']}")
        else:
            print(f"⚠️  Table structure mismatch:")
            for diff in verification['differences']:
                print(f"   - {diff}")
        
        # Get table info
        table_info = loader.get_table_info(result['schema'], result['table'])
        print(f"\n📊 Table info:")
        print(f"   Rows: {table_info['row_count']:,}")
        print(f"   Size: {table_info['table_size']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Failed to create table: {e}")
        logger.error(f"Failed to create table {table_name}: {e}")
        return False


def create_all_tables(drop_if_exists: bool = False):
    """
    Create all tables from config/tables.yaml
    
    Args:
        drop_if_exists: Drop tables if they already exist
    """
    print(f"\n{'=' * 70}")
    print(f"Creating All Tables from config/tables.yaml")
    print(f"{'=' * 70}")
    
    # Load table configuration
    config_file = Path("config/tables.yaml")
    if not config_file.exists():
        print(f"❌ Configuration file not found: {config_file}")
        print(f"   Make sure config/tables.yaml is transferred to this server")
        return False
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    tables = config.get('tables', [])
    
    if not tables:
        print(f"❌ No tables found in config/tables.yaml")
        return False
    
    print(f"\n📋 Found {len(tables)} tables to create:")
    for table_config in tables:
        print(f"   - {table_config['name']}")
    
    # Create each table
    results = {}
    success_count = 0
    fail_count = 0
    
    for table_config in tables:
        table_name = table_config['name']
        
        try:
            success = create_table(table_name, drop_if_exists=drop_if_exists)
            results[table_name] = "success" if success else "failed"
            
            if success:
                success_count += 1
            else:
                fail_count += 1
                
        except Exception as e:
            print(f"\n❌ Error creating {table_name}: {e}")
            results[table_name] = "error"
            fail_count += 1
    
    # Summary
    print(f"\n{'=' * 70}")
    print(f"TABLE CREATION SUMMARY")
    print(f"{'=' * 70}")
    print(f"Total tables: {len(tables)}")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {fail_count}")
    
    print(f"\n📋 Results:")
    for table_name, status in results.items():
        icon = "✅" if status == "success" else "❌"
        print(f"   {icon} {table_name}: {status}")
    
    print(f"{'=' * 70}")
    
    return fail_count == 0


def main():
    parser = argparse.ArgumentParser(
        description="Create PostgreSQL tables from DDL files"
    )
    parser.add_argument(
        "--table",
        help="Create a specific table"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Create all tables from config/tables.yaml"
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop tables if they already exist (recreate)"
    )
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        parser.print_help()
        sys.exit(1)
    
    try:
        print(f"\n{'=' * 70}")
        print(f"PostgreSQL Table Creation Script")
        print(f"{'=' * 70}")
        
        if args.drop_existing:
            print(f"⚠️  WARNING: Existing tables will be dropped and recreated!")
            response = input("Continue? (yes/no): ").strip().lower()
            if response != 'yes':
                print("Cancelled by user")
                sys.exit(0)
        
        if args.table:
            # Create single table
            success = create_table(args.table, drop_if_exists=args.drop_existing)
            
            if success:
                print(f"\n{'=' * 70}")
                print(f"✅ TABLE CREATED SUCCESSFULLY")
                print(f"{'=' * 70}")
                print(f"\n📋 Next step:")
                print(f"   Import data: python scripts/import_data.py --table {args.table}")
            else:
                print(f"\n{'=' * 70}")
                print(f"❌ TABLE CREATION FAILED")
                print(f"{'=' * 70}")
                sys.exit(1)
        else:
            # Create all tables
            success = create_all_tables(drop_if_exists=args.drop_existing)
            
            if success:
                print(f"\n📋 Next step:")
                print(f"   Import data: python scripts/import_data.py --all")
            else:
                print(f"\n⚠️  Some tables failed to create - check logs above")
                sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n\nCancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Script failed: {e}")
        logger.error(f"Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
