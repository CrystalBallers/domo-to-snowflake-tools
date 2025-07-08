#!/usr/bin/env python3
"""
Main CLI for Domo to Snowflake migration tools.

This script provides a unified interface for various migration utilities including:
- Inventory extraction from Google Sheets
- SQL export functionality
- Data migration tools
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import tool modules
try:
    from tools.inventory_handler import export_dataflows_to_sql, InventoryHandler
    from tools.domo_to_snowflake import migrate_dataset, batch_migrate_datasets
    from tools.utils.domo import DomoHandler
    from tools.utils.snowflake import SnowflakeHandler
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    sys.exit(1)

def test_inventory_connection(credentials_path: Optional[str] = None) -> bool:
    """
    Test the Google Sheets connection and show inventory preview.
    
    Args:
        credentials_path: Path to Google Sheets credentials file
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        logger.info("🧪 Testing Google Sheets connection...")
        
        # Use environment variable if no path provided
        if not credentials_path:
            credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        
        if not credentials_path:
            logger.error("❌ No credentials file specified")
            logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
            return False
        
        extractor = InventoryHandler(credentials_path=credentials_path)
        df = extractor.get_inventory()
        
        logger.info("✅ Connection successful!")
        logger.info(f"📊 Inventory preview (first 5 rows):")
        print(df.head())
        
        # Check for dataflow column
        dataflow_column = None
        possible_column_names = ["Dataflow ID", "dataflow", "Dataflow", "DataFlow", "dataflow_id", "Dataflow_ID"]
        
        for col_name in possible_column_names:
            if col_name in df.columns:
                dataflow_column = col_name
                logger.info(f"✅ Found dataflow column: '{col_name}'")
                break
        
        if dataflow_column:
            unique_dataflows = extractor.get_unique_dataflows(df, dataflow_column=dataflow_column)
            logger.info(f"📋 Found {len(unique_dataflows)} dataflows: {unique_dataflows[:10]}...")  # Show first 10
        else:
            logger.warning(f"⚠️  No dataflow column found. Available columns: {list(df.columns)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


def handle_inventory_command(args) -> int:
    """
    Handle the inventory subcommand.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Validate credentials
    credentials_path = args.credentials or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    
    if not credentials_path:
        logger.error("❌ Credentials file not specified")
        logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
        return 1
    
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Credentials file not found: {credentials_path}")
        return 1
    
    # Test connection mode
    if args.test_connection:
        success = test_inventory_connection(credentials_path)
        return 0 if success else 1
    
    # Export mode
    logger.info("🚀 Starting inventory export...")
    logger.info(f"📁 Export directory: {args.export_dir}")
    logger.info(f"🔑 Credentials file: {credentials_path}")
    
    success = export_dataflows_to_sql(
        output_dir=args.export_dir,
        credentials_path=credentials_path
    )
    
    if success:
        logger.info("🎉 Export completed successfully!")
        return 0
    else:
        logger.error("❌ Export failed!")
        return 1


def test_migration_connections() -> bool:
    """
    Test the Domo and Snowflake connections.
    
    Returns:
        bool: True if both connections successful, False otherwise
    """
    try:
        logger.info("🧪 Testing migration connections...")
        
        # Test Domo connection
        logger.info("Testing Domo connection...")
        domo_handler = DomoHandler()
        if domo_handler.setup_auth():
            logger.info("✅ Domo connection successful")
        else:
            logger.error("❌ Domo connection failed")
            return False
        
        # Test Snowflake connection
        logger.info("Testing Snowflake connection...")
        snowflake_handler = SnowflakeHandler()
        if snowflake_handler.setup_connection():
            logger.info("✅ Snowflake connection successful")
            snowflake_handler.cleanup()
        else:
            logger.error("❌ Snowflake connection failed")
            return False
        
        logger.info("🎉 All migration connections tested successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


def handle_migrate_command(args) -> int:
    """
    Handle the migrate subcommand.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Test connection mode
    if args.test_connection:
        success = test_migration_connections()
        return 0 if success else 1
    
    # Single dataset migration
    if args.dataset_id and args.target_table:
        logger.info("🚀 Starting single dataset migration...")
        logger.info(f"📊 Dataset ID: {args.dataset_id}")
        logger.info(f"🎯 Target table: {args.target_table}")
        
        success = migrate_dataset(args.dataset_id, args.target_table)
        
        if success:
            logger.info("🎉 Migration completed successfully!")
            return 0
        else:
            logger.error("❌ Migration failed!")
            return 1
    
    # Batch migration
    if args.batch_file:
        logger.info("🚀 Starting batch migration...")
        logger.info(f"📁 Batch file: {args.batch_file}")
        
        try:
            import json
            
            if not os.path.exists(args.batch_file):
                logger.error(f"❌ Batch file not found: {args.batch_file}")
                return 1
            
            with open(args.batch_file, 'r') as f:
                dataset_mapping = json.load(f)
            
            logger.info(f"📊 Found {len(dataset_mapping)} datasets to migrate")
            
            results = batch_migrate_datasets(dataset_mapping)
            
            if results['failed'] == 0:
                logger.info("🎉 Batch migration completed successfully!")
                return 0
            else:
                logger.error(f"❌ Batch migration completed with {results['failed']} failures!")
                return 1
                
        except Exception as e:
            logger.error(f"❌ Batch migration failed: {e}")
            return 1
    
    # No valid arguments provided
    logger.error("❌ No valid migration options provided")
    logger.error("Use --dataset-id and --target-table for single migration, or --batch-file for batch migration")
    return 1


def create_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser with subcommands.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Domo to Snowflake Migration Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Export inventory dataflows to SQL
    python main.py inventory --export-dir exported_sql
    
    # Test Google Sheets connection
    python main.py inventory --test-connection
    
    # Migrate single dataset
    python main.py migrate --dataset-id 12345 --target-table sales_data
    
    # Batch migrate datasets
    python main.py migrate --batch-file dataset_mapping.json
    
    # Test migration connections
    python main.py migrate --test-connection
    
    # Use custom credentials file
    python main.py inventory --credentials /path/to/creds.json --export-dir output
    
Environment Variables:
    EXPORT_DIR: Default export directory
    GOOGLE_SHEETS_CREDENTIALS_FILE: Path to Google Sheets credentials file
    DOMO_DEVELOPER_TOKEN: Domo API developer token
    DOMO_INSTANCE: Domo instance name
    SNOWFLAKE_ACCOUNT: Snowflake account identifier
    SNOWFLAKE_USER: Snowflake username
    SNOWFLAKE_PASSWORD: Snowflake password
    SNOWFLAKE_WAREHOUSE: Snowflake warehouse name
    SNOWFLAKE_DATABASE: Snowflake database name
    SNOWFLAKE_SCHEMA: Snowflake schema name
        """
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Inventory subcommand
    inventory_parser = subparsers.add_parser(
        'inventory',
        help='Extract and export inventory data from Google Sheets'
    )
    
    inventory_parser.add_argument(
        "--export-dir",
        default=os.getenv("EXPORT_DIR", "exported_sql"),
        help="Directory to save SQL files (default: exported_sql)"
    )
    
    inventory_parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    inventory_parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test Google Sheets connection and show inventory preview"
    )
    
    # Migration subcommand
    migrate_parser = subparsers.add_parser(
        'migrate',
        help='Migrate datasets from Domo to Snowflake'
    )
    
    migrate_parser.add_argument(
        "--dataset-id",
        help="Domo dataset ID to migrate"
    )
    
    migrate_parser.add_argument(
        "--target-table",
        help="Target Snowflake table name"
    )
    
    migrate_parser.add_argument(
        "--batch-file",
        help="JSON file with dataset ID to table name mappings"
    )
    
    migrate_parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test Domo and Snowflake connections"
    )
    
    return parser


def main() -> int:
    """
    Main entry point for the CLI.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    parser = create_parser()
    args = parser.parse_args()
    
    # Handle no command provided
    if not args.command:
        parser.print_help()
        return 1
    
    # Route to appropriate handler
    if args.command == 'inventory':
        return handle_inventory_command(args)
    elif args.command == 'migrate':
        return handle_migrate_command(args)
    
    # If we get here, unknown command
    logger.error(f"❌ Unknown command: {args.command}")
    parser.print_help()
    return 1


if __name__ == "__main__":
    try:
        # Quick test to verify imports work
        logger.info("🔧 Initializing migration tools...")
        
        # Just test that imports work, don't create handlers yet
        logger.info("✅ Tools initialized successfully")
        
        # Run main CLI
        exit_code = main()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.info("⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1) 