#!/usr/bin/env python3
"""
Create Tables Script
Loops through DDL files and creates tables in PostgreSQL if they don't exist

This script should be run on the PostgreSQL server after metadata files are transferred.
Supports both encrypted (obfuscated) and unencrypted DDL files.

Usage:
    python scripts/create_tables.py --all
    python scripts/create_tables.py --table financial_data
    python scripts/create_tables.py --all --drop-existing
    python scripts/create_tables.py --all --password-file ~/.encryption_key
"""
import sys
import argparse
import getpass
import json
import tempfile
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.loaders.postgres_loader import PostgreSQLLoader
from pipeline.transformers.encryptor import FileEncryptor
from pipeline.transformers.obfuscator import DataObfuscator, MetadataObfuscator
from pipeline.config.settings import get_settings
from pipeline.utils.logger import get_logger
import yaml

logger = get_logger(__name__)


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
    return password


def create_table(table_name: str, drop_if_exists: bool = False, password: str = None):
    """
    Create a single table from DDL
    
    Args:
        table_name: Name of the table
        drop_if_exists: Drop table if it already exists
        password: Encryption password (required if files are encrypted)
    """
    print(f"\n{'=' * 70}")
    print(f"Creating Table: {table_name}")
    print(f"{'=' * 70}")
    
    loader = PostgreSQLLoader()
    encryptor = FileEncryptor()
    
    # Import MetadataObfuscator for metadata file ID generation
    from pipeline.transformers.obfuscator import MetadataObfuscator
    obfuscator = MetadataObfuscator()
    
    try:
        # Check if files are encrypted (obfuscated)
        metadata_dir = Path("metadata/schemas")
        ddl_dir = Path("metadata/ddl")
        
        # Try to find unencrypted files first
        metadata_file = metadata_dir / f"{table_name}_metadata.json"
        ddl_file = ddl_dir / f"{table_name}_create.sql"
        
        files_encrypted = False
        
        if not metadata_file.exists() or not ddl_file.exists():
            # Files might be encrypted - generate deterministic IDs
            if not password:
                print(f"❌ Files appear to be encrypted but no password provided")
                print(f"   Use --password-file or set ENCRYPTION_PASSWORD in .env")
                return False
            
            files_encrypted = True
            
            # Generate deterministic file IDs
            metadata_file_id = obfuscator.generate_metadata_file_id(table_name, "metadata")
            ddl_file_id = obfuscator.generate_metadata_file_id(table_name, "ddl")
            
            encrypted_metadata_file = metadata_dir / f"{metadata_file_id}.enc"
            encrypted_ddl_file = ddl_dir / f"{ddl_file_id}.enc"
            
            print(f"🔒 Files are encrypted (obfuscated)")
            print(f"   Metadata file ID: {metadata_file_id}.enc")
            print(f"   DDL file ID: {ddl_file_id}.enc")
            
            if not encrypted_metadata_file.exists():
                print(f"❌ Encrypted metadata file not found: {encrypted_metadata_file}")
                print(f"   Make sure metadata files are transferred to this server")
                return False
            
            if not encrypted_ddl_file.exists():
                print(f"❌ Encrypted DDL file not found: {encrypted_ddl_file}")
                print(f"   Make sure DDL files are transferred to this server")
                return False
            
            print(f"✅ Found encrypted metadata: {encrypted_metadata_file.name}")
            print(f"✅ Found encrypted DDL: {encrypted_ddl_file.name}")
            
            # Decrypt files to temporary location
            print(f"\n🔓 Decrypting files...")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                
                # Decrypt metadata
                temp_metadata = temp_dir_path / f"{table_name}_metadata.json"
                encryptor.decrypt_file(encrypted_metadata_file, temp_metadata, password)
                print(f"   ✅ Decrypted metadata")
                
                # Decrypt DDL
                temp_ddl = temp_dir_path / f"{table_name}_create.sql"
                encryptor.decrypt_file(encrypted_ddl_file, temp_ddl, password)
                print(f"   ✅ Decrypted DDL")
                
                # Load decrypted metadata
                with open(temp_metadata, 'r') as f:
                    metadata = json.load(f)
                
                # Load decrypted DDL
                with open(temp_ddl, 'r') as f:
                    ddl = f.read()
                
                # Extract schema and table from DDL
                import re
                match = re.search(r'CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)', ddl)
                if match:
                    schema = match.group(1)
                    table = match.group(2)
                else:
                    print(f"❌ Could not parse schema/table from DDL")
                    return False
                
                print(f"\n🔄 Creating table {schema}.{table}...")
                
                # Create PostgreSQL connection and execute DDL
                import psycopg2
                from pipeline.config.settings import get_postgres_connection_params
                
                conn_params = get_postgres_connection_params()
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()
                
                try:
                    # Create schema if it doesn't exist
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    
                    # Drop table if requested
                    if drop_if_exists:
                        cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")
                        print(f"   Dropped existing table {schema}.{table}")
                    
                    # Execute DDL
                    cursor.execute(ddl)
                    conn.commit()
                    
                    print(f"\n✅ Table created successfully!")
                    print(f"   Schema: {schema}")
                    print(f"   Table: {table}")
                    print(f"   Columns: {len(metadata['columns']) + 1} (including data_inserted_at)")
                    
                    # Get table info
                    cursor.execute(f'SELECT COUNT(*) FROM {schema}.{table}')
                    row_count = cursor.fetchone()[0]
                    
                    cursor.execute(f"""
                        SELECT pg_size_pretty(pg_total_relation_size('{schema}.{table}'))
                    """)
                    table_size = cursor.fetchone()[0]
                    
                    print(f"\n📊 Table info:")
                    print(f"   Rows: {row_count:,}")
                    print(f"   Size: {table_size}")
                    
                    return True
                    
                except Exception as e:
                    conn.rollback()
                    print(f"\n❌ Failed to create table: {e}")
                    return False
                finally:
                    cursor.close()
                    conn.close()
        
        else:
            # Files are not encrypted - use original logic
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
            import psycopg2
            from pipeline.config.settings import get_postgres_connection_params
            
            conn_params = get_postgres_connection_params()
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT COUNT(*) FROM {result['schema']}.{result['table']}")
            row_count = cursor.fetchone()[0]
            
            cursor.execute(f"""
                SELECT pg_size_pretty(pg_total_relation_size('{result['schema']}.{result['table']}'))
            """)
            table_size = cursor.fetchone()[0]
            
            print(f"\n📊 Table info:")
            print(f"   Rows: {row_count:,}")
            print(f"   Size: {table_size}")
            
            cursor.close()
            conn.close()
            
            return True
        
    except Exception as e:
        print(f"\n❌ Failed to create table: {e}")
        logger.error(f"Failed to create table {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_all_tables(drop_if_exists: bool = False, password: str = None):
    """
    Create all tables from config/tables.yaml
    
    Args:
        drop_if_exists: Drop tables if they already exist
        password: Encryption password (required if files are encrypted)
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
            success = create_table(table_name, drop_if_exists=drop_if_exists, password=password)
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
        description="Create PostgreSQL tables from DDL files (supports encrypted files)"
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
    parser.add_argument(
        "--password-file",
        help="Path to file containing encryption password (for encrypted DDL files)"
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
        
        # Get password if needed (priority: --password-file > ENCRYPTION_PASSWORD env > prompt if encrypted files detected)
        settings = get_settings()
        env_password = getattr(settings, 'encryption_password', None)
        password = None
        
        # Check if files are encrypted by looking for .enc files
        ddl_dir = Path("metadata/ddl")
        encrypted_files = list(ddl_dir.glob("*.enc"))
        
        if encrypted_files:
            print(f"\n🔒 Detected {len(encrypted_files)} encrypted DDL files")
            password = get_password(args.password_file, from_env=env_password)
        else:
            print(f"\n📁 Using unencrypted DDL files")
        
        if args.drop_existing:
            print(f"\n⚠️  WARNING: Existing tables will be dropped and recreated!")
            response = input("Continue? (yes/no): ").strip().lower()
            if response != 'yes':
                print("Cancelled by user")
                sys.exit(0)
        
        if args.table:
            # Create single table
            success = create_table(args.table, drop_if_exists=args.drop_existing, password=password)
            
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
            success = create_all_tables(drop_if_exists=args.drop_existing, password=password)
            
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
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
