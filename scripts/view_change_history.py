"""
View Change History Script
View metadata change history for tables
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils.change_logger import ChangeLogger
from pipeline.utils.logger import get_logger
from pipeline.config.settings import get_settings
from pipeline.transformers.obfuscator import MetadataObfuscator
import getpass

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
        description="View metadata change history for tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # View all changes for a table (obfuscation enabled by default)
  python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO
  
  # View last 5 changes
  python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO --limit 5
  
  # View changes in date range
  python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO --from 2024-01-01 --to 2024-01-31
  
  # View summary of all tables with changes
  python scripts/view_change_history.py --summary
  
  # View non-obfuscated change logs
  python scripts/view_change_history.py --table FUND_SHARE_CLASS_BASIC_INFO --no-obfuscate
        """
    )
    
    parser.add_argument(
        '--table',
        type=str,
        help='Table name to view change history for'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of entries to display (most recent first)'
    )
    
    parser.add_argument(
        '--from',
        dest='from_date',
        type=str,
        help='Start date for filtering (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--to',
        dest='to_date',
        type=str,
        help='End date for filtering (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary of all tables with changes'
    )
    
    parser.add_argument(
        '--password',
        type=str,
        help='Decryption password (for encrypted change logs, will use .env or prompt if not provided)'
    )
    
    parser.add_argument(
        '--no-obfuscate',
        action='store_true',
        help='Change logs are NOT encrypted/obfuscated (obfuscation enabled by default)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.table and not args.summary:
        parser.error("Must specify either --table or --summary")
    
    if args.table and args.summary:
        parser.error("Cannot specify both --table and --summary")
    
    # Determine if obfuscation is enabled (default: True, based on settings)
    settings = get_settings()
    obfuscated = not args.no_obfuscate and settings.obfuscate_names
    
    # Get password and obfuscator if obfuscation enabled
    password = None
    obfuscator = None
    if obfuscated:
        password = get_password_from_env_or_prompt(args.password)
        obfuscator = MetadataObfuscator()
    
    # Initialize change logger
    change_logger = ChangeLogger(obfuscator=obfuscator)
    
    try:
        if args.summary:
            # Show summary of all tables
            show_summary(change_logger)
        
        elif args.table:
            # Show change history for specific table
            if args.from_date or args.to_date:
                # Date range filtering
                show_changes_by_date_range(
                    change_logger,
                    args.table,
                    args.from_date,
                    args.to_date,
                    password
                )
            else:
                # Show all or limited changes
                show_changes(change_logger, args.table, args.limit, password)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return 1


def show_changes(change_logger: ChangeLogger, table_name: str, limit: int = None, password: str = None):
    """Show change history for a table"""
    logger.info(f"Retrieving change history for {table_name}...")
    
    entries = change_logger.get_change_history(table_name, limit=limit, password=password)
    
    if not entries:
        print(f"\nNo change history found for {table_name}")
        print("\nThis could mean:")
        print("  1. No metadata changes have been detected yet")
        print("  2. Change tracking was not enabled (use --check-changes flag)")
        print("  3. The table name is incorrect")
        return
    
    # Display header
    print("\n" + "=" * 80)
    print(f"CHANGE HISTORY: {table_name}")
    print("=" * 80)
    
    if limit:
        print(f"\nShowing last {min(len(entries), limit)} entries (most recent first)\n")
    else:
        print(f"\nShowing all {len(entries)} entries (most recent first)\n")
    
    # Display entries
    for i, entry in enumerate(entries, 1):
        print(f"Entry {i}:")
        print(entry)
        print()
    
    print("=" * 80)


def show_changes_by_date_range(
    change_logger: ChangeLogger,
    table_name: str,
    from_date_str: str = None,
    to_date_str: str = None,
    password: str = None
):
    """Show changes within a date range"""
    # Parse dates
    if from_date_str:
        try:
            from_date = datetime.fromisoformat(from_date_str)
        except ValueError:
            logger.error(f"Invalid from date format: {from_date_str}. Use YYYY-MM-DD")
            return
    else:
        from_date = datetime.min
    
    if to_date_str:
        try:
            to_date = datetime.fromisoformat(to_date_str)
        except ValueError:
            logger.error(f"Invalid to date format: {to_date_str}. Use YYYY-MM-DD")
            return
    else:
        to_date = datetime.max
    
    logger.info(f"Retrieving changes for {table_name} from {from_date.date()} to {to_date.date()}...")
    
    entries = change_logger.get_changes_by_date_range(table_name, from_date, to_date, password)
    
    if not entries:
        print(f"\nNo changes found for {table_name} in the specified date range")
        return
    
    # Display header
    print("\n" + "=" * 80)
    print(f"CHANGE HISTORY: {table_name}")
    print(f"Date Range: {from_date.date()} to {to_date.date()}")
    print("=" * 80)
    print(f"\nFound {len(entries)} entries\n")
    
    # Display entries
    for i, entry in enumerate(entries, 1):
        print(f"Entry {i}:")
        print(entry)
        print()
    
    print("=" * 80)


def show_summary(change_logger: ChangeLogger):
    """Show summary of all tables with changes"""
    logger.info("Scanning for tables with change history...")
    
    # Find all change log files
    changes_dir = Path("metadata/changes")
    
    if not changes_dir.exists():
        print("\nNo change logs found")
        print("\nChange tracking may not be enabled.")
        print("Use --check-changes flag when extracting metadata.")
        return
    
    log_files = list(changes_dir.glob("*_changes.log"))
    
    if not log_files:
        print("\nNo change logs found")
        return
    
    # Get summary for each table
    summaries = []
    for log_file in log_files:
        table_name = log_file.stem.replace('_changes', '')
        summary = change_logger.get_change_summary(table_name)
        summaries.append(summary)
    
    # Sort by total changes (descending)
    summaries.sort(key=lambda x: x['total_changes'], reverse=True)
    
    # Display summary
    print("\n" + "=" * 80)
    print("CHANGE HISTORY SUMMARY")
    print("=" * 80)
    print(f"\nTotal tables with change logs: {len(summaries)}\n")
    
    print(f"{'Table Name':<50} {'Changes':<10} {'Total Entries':<15}")
    print("-" * 80)
    
    for summary in summaries:
        table_name = summary['table_name']
        changes = summary['total_changes']
        total = summary['total_entries']
        
        print(f"{table_name:<50} {changes:<10} {total:<15}")
    
    print("\n" + "=" * 80)
    print("\nTo view details for a specific table:")
    print("  python scripts/view_change_history.py --table <TABLE_NAME>")
    print()


if __name__ == "__main__":
    sys.exit(main())
