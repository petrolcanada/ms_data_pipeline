"""
Decrypt Metadata Script
Decrypt encrypted metadata files for human viewing
"""
import argparse
import sys
import getpass
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils.metadata_decryptor import MetadataDecryptor
from pipeline.utils.change_logger import ChangeLogger
from pipeline.utils.logger import get_logger
from pipeline.config.settings import get_settings

logger = get_logger(__name__)


def get_password_from_env_or_prompt(password_arg: str = None) -> str:
    """
    Get password from argument, .env file, or prompt
    
    Priority: command line argument > .env file > prompt
    """
    # Try command line argument first
    if password_arg:
        return password_arg
    
    # Try .env file
    settings = get_settings()
    if settings.encryption_password:
        logger.info("Using encryption password from .env file")
        return settings.encryption_password
    
    # Prompt for password
    return getpass.getpass("Enter decryption password: ")


def main():
    parser = argparse.ArgumentParser(
        description="Decrypt encrypted metadata files for human viewing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Decrypt all tables
  python scripts/decrypt_metadata.py --all --password mypassword
  
  # Decrypt specific table
  python scripts/decrypt_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO --password mypassword
  
  # Prompt for password (secure)
  python scripts/decrypt_metadata.py --all
  
  # List available tables
  python scripts/decrypt_metadata.py --list --password mypassword
  
  # Clean up decrypted files
  python scripts/decrypt_metadata.py --clean
  
  # Decrypt and show change history
  python scripts/decrypt_metadata.py --table FUND_SHARE_CLASS_BASIC_INFO --password mypassword --show-changes
        """
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Decrypt all tables from master index'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        help='Decrypt specific table by name'
    )
    
    parser.add_argument(
        '--password',
        type=str,
        help='Decryption password (will prompt if not provided)'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available tables in master index'
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Delete all decrypted files'
    )
    
    parser.add_argument(
        '--show-changes',
        action='store_true',
        help='Display change history after decrypting'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='metadata/decrypted',
        help='Custom output directory (default: metadata/decrypted)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not any([args.all, args.table, args.list, args.clean]):
        parser.error("Must specify one of: --all, --table, --list, or --clean")
    
    if args.clean:
        # Clean up decrypted files
        clean_decrypted_files(args.output_dir)
        return 0
    
    # Get password if needed
    password = None
    if not args.clean:
        password = get_password_from_env_or_prompt(args.password)
    
    # Initialize decryptor
    decrypted_dir = Path(args.output_dir)
    decryptor = MetadataDecryptor(decrypted_dir=decrypted_dir)
    
    # Ensure .gitignore is updated
    decryptor.ensure_gitignore()
    
    try:
        if args.list:
            # List available tables
            list_tables(decryptor, password)
        
        elif args.all:
            # Decrypt all tables
            decrypt_all_tables(decryptor, password, args.show_changes)
        
        elif args.table:
            # Decrypt specific table
            decrypt_single_table(decryptor, args.table, password, args.show_changes)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        return 1
    except ValueError as e:
        logger.error(f"❌ {e}")
        return 1
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return 1


def list_tables(decryptor: MetadataDecryptor, password: str):
    """List available tables from config/tables.yaml"""
    logger.info("Listing available tables from config/tables.yaml...")
    
    try:
        import yaml
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        tables = [t['name'] for t in config['tables']]
        
        print("\n" + "=" * 80)
        print("AVAILABLE TABLES")
        print("=" * 80)
        print(f"\nTotal tables: {len(tables)}\n")
        
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table}")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        raise


def decrypt_all_tables(decryptor: MetadataDecryptor, password: str, show_changes: bool = False):
    """Decrypt all tables from config/tables.yaml"""
    logger.info("Decrypting all tables from config/tables.yaml...")
    
    try:
        import yaml
        with open("config/tables.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        tables = [t['name'] for t in config['tables']]
        results = {}
        
        for table_name in tables:
            try:
                result = decryptor.decrypt_table(table_name, password)
                results[table_name] = result
            except Exception as e:
                logger.error(f"Failed to decrypt {table_name}: {e}")
                results[table_name] = {
                    "table_name": table_name,
                    "status": "error",
                    "error": str(e)
                }
        
        # Display summary
        print("\n" + "=" * 80)
        print("DECRYPTION SUMMARY")
        print("=" * 80)
        print()
        
        success_count = 0
        error_count = 0
        
        for table_name, result in results.items():
            if result.get('status') == 'success':
                success_count += 1
                print(f"✅ {table_name}")
                print(f"   Metadata: {result['decrypted_files']['metadata']}")
                print(f"   DDL: {result['decrypted_files']['ddl']}")
                if result.get('has_change_log'):
                    print(f"   Changes: {result['decrypted_files']['changes']}")
                if result.get('archived_count', 0) > 0:
                    print(f"   Archived versions: {result['archived_count']}")
                print(f"   Columns: {result['metadata_summary']['columns']}")
                print()
            else:
                error_count += 1
                print(f"❌ {table_name}")
                print(f"   Error: {result.get('error', 'Unknown error')}")
                print()
        
        print("=" * 80)
        print(f"Total: {len(results)} tables")
        print(f"Success: {success_count}")
        print(f"Errors: {error_count}")
        print("=" * 80)
        
        # Show change history if requested
        if show_changes:
            print("\nChange history available for decrypted tables.")
            print("Use: python scripts/view_change_history.py --table <TABLE_NAME>")
        
    except Exception as e:
        logger.error(f"Failed to decrypt all tables: {e}")
        raise


def decrypt_single_table(decryptor: MetadataDecryptor, table_name: str, password: str, show_changes: bool = False):
    """Decrypt a single table"""
    logger.info(f"Decrypting table: {table_name}")
    
    try:
        result = decryptor.decrypt_table(table_name, password)
        
        # Display result
        print("\n" + "=" * 80)
        print(f"DECRYPTED: {table_name}")
        print("=" * 80)
        print()
        print(f"Metadata: {result['decrypted_files']['metadata']}")
        print(f"DDL: {result['decrypted_files']['ddl']}")
        if result.get('has_change_log'):
            print(f"Changes: {result['decrypted_files']['changes']}")
        print()
        print("Metadata Summary:")
        print(f"  Columns: {result['metadata_summary']['columns']}")
        print(f"  Rows: {result['metadata_summary']['row_count']:,}")
        print(f"  Last Altered: {result['metadata_summary']['last_altered']}")
        print()
        print("=" * 80)
        
        # Show change history if requested
        if show_changes:
            show_change_history(table_name)
        
    except Exception as e:
        logger.error(f"Failed to decrypt {table_name}: {e}")
        raise


def show_change_history(table_name: str):
    """Display change history for a table"""
    change_logger = ChangeLogger()
    
    print("\n" + "=" * 80)
    print(f"CHANGE HISTORY: {table_name}")
    print("=" * 80)
    print()
    
    entries = change_logger.get_change_history(table_name, limit=5)
    
    if not entries:
        print("No change history found")
    else:
        for i, entry in enumerate(entries, 1):
            print(f"Entry {i}:")
            print(entry)
            print()
    
    print("=" * 80)
    print(f"\nShowing last {min(len(entries), 5)} entries")
    print("For full history, use: python scripts/view_change_history.py --table", table_name)
    print()


def clean_decrypted_files(output_dir: str):
    """Clean up all decrypted files"""
    decrypted_dir = Path(output_dir)
    decryptor = MetadataDecryptor(decrypted_dir=decrypted_dir)
    
    logger.info("Cleaning up decrypted files...")
    
    try:
        result = decryptor.clean_decrypted_files()
        
        print("\n" + "=" * 80)
        print("CLEANUP COMPLETE")
        print("=" * 80)
        print()
        print(f"Deleted {result['deleted_files']} file(s):")
        print(f"  Metadata files: {result.get('metadata_files', 0)}")
        print(f"  DDL files: {result.get('ddl_files', 0)}")
        print(f"  Change log files: {result.get('changes_files', 0)}")
        print(f"  Index files: {result.get('index_files', 0)}")
        print()
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"Failed to clean decrypted files: {e}")
        raise


if __name__ == "__main__":
    sys.exit(main())
