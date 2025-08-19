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
    from tools.domo_to_snowflake import (
        migrate_dataset, 
        batch_migrate_datasets, 
        migrate_from_spreadsheet, 
        MigrationManager
    )
    from tools.utils import DomoHandler, SnowflakeHandler, DatasetComparator
    from tools.utils import show_mfa_debug_info, reload_environment
    from tools.utils.domo import export_datasets_to_spreadsheet, DomoHandler
    from tools.get_all_stg_files import get_stg_files_data, generate_stg_files_from_dataframe
    from tools.utils.create_source import generate_sources_from_spreadsheet
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
    Test the Domo and Snowflake connections using MigrationManager.
    
    Returns:
        bool: True if both connections successful, False otherwise
    """
    try:
        logger.info("🧪 Testing migration connections...")
        
        # Show TOTP debug info if using MFA
        show_mfa_debug_info()
        
        # Use MigrationManager to test connections
        with MigrationManager() as manager:
            logger.info("✅ All migration connections tested successfully!")
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
    # Handle reload-env argument
    if args.reload_env:
        reload_environment()
    
    # Test connection mode
    if args.test_connection:
        success = test_migration_connections()
        return 0 if success else 1
    
    # Spreadsheet migration
    if args.from_spreadsheet:
        logger.info("🚀 Starting spreadsheet-based migration...")
        logger.info(f"📋 Spreadsheet ID: {args.spreadsheet_id}")
        logger.info(f"📄 Sheet name: {args.sheet_name}")
        
        if args.full_table:
            logger.info("📊 Full table mode: Will upload entire datasets (no row limit)")
        elif args.auto_chunk_size:
            logger.info("📊 X-Small optimized auto-chunk mode: Will automatically determine optimal chunk size based on dataset size for X-Small warehouse")
        else:
            logger.info("📊 Limited mode: Will upload first 1000 rows per dataset")
        
        logger.info("🔧 Column normalization: Automatic Snowflake compatibility (UPPERCASE)")
        
        # Show TOTP debug info if using MFA
        show_mfa_debug_info()
        
        results = migrate_from_spreadsheet(
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            credentials_path=args.credentials,
            full_table=args.full_table,
            auto_chunk_size=args.auto_chunk_size
        )
        
        if 'errors' in results and results['errors']:
            logger.error("❌ Spreadsheet migration failed due to errors:")
            for error in results['errors']:
                logger.error(f"   - {error}")
            return 1
        elif results['failed'] == 0:
            logger.info("🎉 Spreadsheet migration completed successfully!")
            return 0
        else:
            logger.error(f"❌ Spreadsheet migration completed with {results['failed']} failures!")
            return 1
    
    # Single dataset migration
    if args.dataset_id and args.target_table:
        logger.info("🚀 Starting single dataset migration...")
        logger.info(f"📊 Dataset ID: {args.dataset_id}")
        logger.info(f"🎯 Target table: {args.target_table}")
        
        # Show TOTP debug info if using MFA
        show_mfa_debug_info()
        
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
        
        # Show TOTP debug info if using MFA
        show_mfa_debug_info()
        
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
    logger.error("Use --dataset-id and --target-table for single migration, --batch-file for batch migration, or --from-spreadsheet for spreadsheet migration")
    return 1


def handle_datasets_command(args) -> int:
    """
    Handle the datasets subcommand.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Test connection mode
    if args.test_connection:
        logger.info("🧪 Testing Domo connection...")
        try:
            domo_handler = DomoHandler()
            if domo_handler.setup_auth():
                logger.info("✅ Domo connection successful")
                return 0
            else:
                logger.error("❌ Domo connection failed")
                return 1
        except Exception as e:
            logger.error(f"❌ Domo connection test failed: {e}")
            return 1
    
    # Export datasets to spreadsheet
    if args.export_to_spreadsheet:
        print("test \n\n\n")
        logger.info("🚀 Starting dataset export to spreadsheet...")
        logger.info(f"📋 Spreadsheet ID: {args.spreadsheet_id}")
        logger.info(f"📄 Sheet name: {args.sheet_name}")
        
        success = export_datasets_to_spreadsheet(
            spreadsheet_id=args.spreadsheet_id,
            sheet_name=args.sheet_name,
            credentials_path=args.credentials
        )
        
        if success:
            logger.info("🎉 Dataset export completed successfully!")
            return 0
        else:
            logger.error("❌ Dataset export failed!")
            return 1
    
    # List datasets locally
    if args.list_local:
        logger.info("📋 Fetching all datasets from Domo...")
        
        # Initialize Domo handler
        domo_handler = DomoHandler()
        if not domo_handler.setup_auth():
            logger.error("❌ Failed to authenticate with Domo")
            return 1
        
        datasets = domo_handler.get_all_datasets(batch_size=args.batch_size)
        
        if not datasets:
            logger.error("❌ No datasets found")
            return 1
        
        logger.info(f"📊 Found {len(datasets)} datasets:")
        for dataset in datasets:
            logger.info(f"   {dataset['id']}: {dataset['name']}")
        
        return 0
    
    # No valid arguments provided
    logger.error("❌ No valid dataset options provided")
    logger.error("Use --export-to-spreadsheet to export to Google Sheets, --list-local to list locally, or --test-connection to test Domo connection")
    return 1


def handle_compare_command(args) -> int:
    """
    Handle the compare subcommand for comparing Domo datasets with Snowflake tables.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Handle spreadsheet mode
    if args.from_spreadsheet:
        return handle_compare_from_spreadsheet(args)
    
    # Handle inventory mode
    if args.from_inventory:
        return handle_compare_from_inventory(args)
    
    # Validate required arguments for single comparison
    if not args.domo_dataset_id:
        logger.error("❌ Domo dataset ID is required")
        logger.error("Use --domo-dataset-id to specify the dataset to compare")
        return 1
    
    if not args.snowflake_table:
        logger.error("❌ Snowflake table name is required")
        logger.error("Use --snowflake-table to specify the table to compare")
        return 1
    
    if not args.key_columns:
        logger.error("❌ Key columns are required for comparison")
        logger.error("Use --key-columns to specify one or more key columns")
        return 1
    
    # Test connection mode
    if args.test_connection:
        logger.info("🧪 Testing connections for comparison...")
        try:
            comparator = DatasetComparator()
            success = comparator.setup_connections()
            if success:
                logger.info("✅ All connections for comparison tested successfully!")
                return 0
            else:
                logger.error("❌ Connection test failed!")
                return 1
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return 1
        finally:
            try:
                comparator.cleanup()
            except:
                pass
    
    # Perform comparison
    logger.info("🚀 Starting Domo vs Snowflake comparison...")
    logger.info(f"📊 Domo Dataset ID: {args.domo_dataset_id}")
    logger.info(f"❄️  Snowflake Table: {args.snowflake_table}")
    logger.info(f"🔑 Key Columns: {', '.join(args.key_columns)}")
    
    if args.sample_size:
        logger.info(f"📏 Sample Size: {args.sample_size:,}")
    else:
        logger.info(f"📏 Sample Size: Automatic calculation")
    
    if args.transform_columns:
        logger.info(f"🔄 Column Name Transformation: Enabled")
    else:
        logger.info(f"🔄 Column Name Transformation: Disabled")
    
    logger.info(f"🎲 Sampling Method: {args.sampling_method}")
    
    try:
        # Initialize the comparator
        comparator = DatasetComparator()
        
        # Setup connections
        if not comparator.setup_connections():
            logger.error("❌ Failed to setup connections")
            return 1
        
        # Generate comparison report
        report = comparator.generate_report(
            domo_dataset_id=args.domo_dataset_id,
            snowflake_table=args.snowflake_table,
            key_columns=args.key_columns,
            sample_size=args.sample_size,
            transform_names=args.transform_columns,
            sampling_method=args.sampling_method
        )
        
        # Print the report
        comparator.print_report(report)
        
        # Determine exit code based on comparison results
        if report.get('errors'):
            logger.error("❌ Comparison completed with errors")
            return 1
        elif report.get('overall_match', False):
            logger.info("🎉 Comparison completed successfully - datasets match!")
            return 0
        else:
            logger.warning("⚠️  Comparison completed - discrepancies found")
            return 0  # Not an error, just differences found
        
    except KeyboardInterrupt:
        logger.info("⚠️  Comparison cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Comparison failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify that the Domo dataset ID is correct")
        logger.error("   - Verify that the Snowflake table exists and is accessible")
        logger.error("   - Verify that the key columns exist in both sources")
        logger.error("   - Check your connection credentials")
        return 1
    finally:
        try:
            comparator.cleanup()
        except:
            pass  # Ignore cleanup errors


def handle_compare_from_spreadsheet(args) -> int:
    """
    Handle the compare subcommand for comparing datasets from Google Sheets.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info("🚀 Starting spreadsheet-based comparisons...")
    
    # Get spreadsheet ID from args or environment
    from tools.utils.common import get_env_config
    env_config = get_env_config()
    spreadsheet_id = args.spreadsheet_id or env_config.get("MIGRATION_SPREADSHEET_ID")
    
    # Validate spreadsheet arguments
    if not spreadsheet_id:
        logger.error("❌ Spreadsheet ID is required for spreadsheet-based comparisons")
        logger.error("Set MIGRATION_SPREADSHEET_ID environment variable or use --spreadsheet-id")
        return 1
    
    try:
        # Initialize the comparator
        comparator = DatasetComparator()
        
        # Run comparisons from spreadsheet
        results = comparator.compare_from_spreadsheet(
            spreadsheet_id=spreadsheet_id,
            sheet_name=args.sheet_name,
            credentials_path=args.credentials,
            sampling_method=args.sampling_method
        )
        
        # Determine exit code based on results
        if 'errors' in results and results['errors']:
            logger.error("❌ Spreadsheet comparison failed due to errors:")
            for error in results['errors'][:5]:  # Show first 5 errors
                logger.error(f"   - {error}")
            if len(results['errors']) > 5:
                logger.error(f"   ... and {len(results['errors']) - 5} more errors")
            return 1
        elif results['failed'] == 0:
            logger.info("🎉 All spreadsheet comparisons completed successfully!")
            return 0
        else:
            logger.warning(f"⚠️  Spreadsheet comparisons completed with {results['failed']} failures!")
            return 0  # Not an error, just differences found
        
    except KeyboardInterrupt:
        logger.info("⚠️  Spreadsheet comparison cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Spreadsheet comparison failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify that the spreadsheet ID is correct")
        logger.error("   - Verify that the sheet name exists")
        logger.error("   - Check that required columns exist (Output ID, Table Name, Key Columns)")
        logger.error("   - Verify your Google Sheets credentials")
        return 1
    finally:
        try:
            comparator.cleanup()
        except:
            pass  # Ignore cleanup errors


def handle_compare_from_inventory(args) -> int:
    """
    Handle the compare subcommand for comparing datasets from inventory spreadsheet.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info("🚀 Starting inventory-based comparisons...")
    logger.info("📋 Using existing inventory spreadsheet configuration")
    
    try:
        # Initialize the comparator
        comparator = DatasetComparator()
        
        # Run comparisons from inventory
        results = comparator.compare_from_inventory(
            credentials_path=args.credentials,
            sampling_method=args.sampling_method
        )
        
        # Determine exit code based on results
        if 'errors' in results and results['errors']:
            logger.error("❌ Inventory comparison failed due to errors:")
            for error in results['errors'][:5]:  # Show first 5 errors
                logger.error(f"   - {error}")
            if len(results['errors']) > 5:
                logger.error(f"   ... and {len(results['errors']) - 5} more errors")
            return 1
        elif results['failed'] == 0:
            logger.info("🎉 All inventory comparisons completed successfully!")
            return 0
        else:
            logger.warning(f"⚠️  Inventory comparisons completed with {results['failed']} failures!")
            return 0  # Not an error, just differences found
        
    except KeyboardInterrupt:
        logger.info("⚠️  Inventory comparison cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Inventory comparison failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify that MIGRATION_SPREADSHEET_ID environment variable is set")
        logger.error("   - Check that inventory sheet has required columns (Output ID, Model Name, Key Columns)")
        logger.error("   - Verify your Google Sheets credentials")
        return 1
    finally:
        try:
            comparator.cleanup()
        except:
            pass  # Ignore cleanup errors


def handle_generate_stg_command(args) -> int:
    """
    Handle the generate-stg subcommand for generating staging SQL files.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Validate required configuration
    if not args.database:
        logger.error("❌ Database not specified. Use --database or set SNOWFLAKE_DATABASE environment variable.")
        return 1
    
    if not args.credentials:
        logger.error("❌ Google Sheets credentials not specified. Use --credentials or set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable.")
        return 1
        
    if not args.spreadsheet_id:
        logger.error("❌ Spreadsheet ID not specified. Use --spreadsheet-id or set MIGRATION_SPREADSHEET_ID environment variable.")
        return 1
    
    # Show configuration
    logger.info("🚀 Starting staging files generation...")
    logger.info(f"📊 Database: {args.database}")
    logger.info(f"📂 Schema: {args.schema}")
    logger.info(f"👤 Role: {args.role}")
    logger.info(f"🏠 Warehouse: {args.warehouse}")
    logger.info(f"📁 Output: {args.output_dir}")
    logger.info(f"📄 Spreadsheet: {args.spreadsheet_id}")
    
    if args.read_only:
        logger.info("⚠️  Read-only mode: Will not update Check column")
    
    if args.dry_run:
        logger.info("🧪 Dry-run mode: Will not create files or update sheets")
    
    try:
        # Get the data and Google Sheets client
        df, gsheets, spreadsheet_id = get_stg_files_data()
        
        if df.empty:
            logger.error("❌ Could not obtain data from spreadsheet.")
            return 1

        logger.info("✅ Data extracted successfully.")
        
        if gsheets and spreadsheet_id and not args.read_only:
            logger.info("✅ Google Sheets write permissions confirmed.")
        else:
            logger.info("⚠️  Google Sheets updates disabled.")
            gsheets = None  # Disable writing if read-only mode
        
        if args.dry_run:
            logger.info("🧪 Dry-run mode - showing what would be processed:")
            pending_rows = df[df['Check'].astype(str).str.lower() != 'true']
            completed_rows = df[df['Check'].astype(str).str.lower() == 'true']
            
            logger.info(f"   ✅ Already completed: {len(completed_rows)} files")
            logger.info(f"   🔄 Would process: {len(pending_rows)} files")
            
            if not pending_rows.empty:
                logger.info("📋 Files that would be generated:")
                for _, row in pending_rows.head(10).iterrows():
                    logger.info(f"   • {row['Model']} (from {row['Name']})")
                if len(pending_rows) > 10:
                    logger.info(f"   ... and {len(pending_rows) - 10} more files")
            
            logger.info("🧪 Dry-run completed. Use without --dry-run to actually generate files.")
            return 0
        
        # Generate SQL files automatically
        logger.info("🔄 Starting automatic SQL file generation...")
        generate_stg_files_from_dataframe(
            df=df,
            database=args.database,
            schema=args.schema,
            output_dir=args.output_dir,
            role=args.role,
            warehouse=args.warehouse,
            gsheets=gsheets, 
            spreadsheet_id=spreadsheet_id
        )
        
        logger.info("🎉 Process completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("⚠️  STG generation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ STG generation failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify that the spreadsheet ID and credentials are correct")
        logger.error("   - Check that the 'Stg Files' tab exists in the spreadsheet")
        logger.error("   - Verify Snowflake connection credentials and permissions")
        logger.error("   - Ensure the database and schema exist in Snowflake")
        return 1


def handle_generate_sources_command(args) -> int:
    """
    Handle the generate-sources subcommand for generating dbt sources.yml files.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Validate required configuration
    if not args.database:
        logger.error("❌ Database not specified. Use --database or set SNOWFLAKE_DATABASE environment variable.")
        return 1
    
    if not args.schema:
        logger.error("❌ Schema not specified. Use --schema.")
        return 1
    
    # Show configuration
    logger.info("🚀 Starting dbt sources.yml generation...")
    logger.info(f"📊 Database: {args.database}")
    logger.info(f"📂 Schema: {args.schema}")
    logger.info(f"📁 Output: {args.output}")
    
    try:
        # Generate sources file from Google Sheets
        success = generate_sources_from_spreadsheet(
            database=args.database,
            schema=args.schema,
            output_file=args.output
        )
        
        if success:
            logger.info("🎉 Sources file generated successfully!")
            return 0
        else:
            logger.error("❌ Sources generation failed!")
            return 1
        
    except KeyboardInterrupt:
        logger.info("⚠️  Sources generation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Sources generation failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify Google Sheets credentials and spreadsheet access")
        logger.error("   - Check that the 'Stg Files' tab exists in the spreadsheet")
        logger.error("   - Ensure the 'Name' column contains valid table names")
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
    python main.py inventory --export-dir results/sql/translated
    
    # Test Google Sheets connection
    python main.py inventory --test-connection
    
    # Migrate single dataset
    python main.py migrate --dataset-id 12345 --target-table sales_data
    
    # Batch migrate datasets from JSON file
    python main.py migrate --batch-file dataset_mapping.json
    
    # Migrate datasets from Google Sheets Migration tab
    python main.py migrate --from-spreadsheet
    
    # Migrate from spreadsheet with custom credentials
    python main.py migrate --from-spreadsheet --credentials /path/to/creds.json
    
    # Migrate from custom spreadsheet and sheet
    python main.py migrate --from-spreadsheet --spreadsheet-id YOUR_SHEET_ID --sheet-name MyMigration
    
    # Migrate entire tables (no row limit) from spreadsheet
    python main.py migrate --from-spreadsheet --full-table
    
    # Use X-Small optimized chunking for large datasets
    python main.py migrate --from-spreadsheet --auto-chunk-size
    
    # Test migration connections
    python main.py migrate --test-connection
    
    # Compare Domo dataset with Snowflake table
    python main.py compare --domo-dataset-id 12345 --snowflake-table sales_data --key-columns id date
    
    # Compare with custom sample size and column transformation
    python main.py compare --domo-dataset-id 12345 --snowflake-table sales_data --key-columns id --sample-size 5000 --transform-columns
    
    # Compare multiple datasets from Google Sheets
    python main.py compare --from-spreadsheet
    
    # Compare from custom spreadsheet and sheet
    python main.py compare --from-spreadsheet --spreadsheet-id YOUR_SHEET_ID --sheet-name "QA - Test"
    
    # Compare datasets from existing inventory spreadsheet
    python main.py compare --from-inventory
    
    # Test comparison connections
    python main.py compare --test-connection
    
    # Use custom credentials file
    python main.py inventory --credentials /path/to/creds.json --export-dir output

Features:
    🔧 Automatic column normalization for Snowflake compatibility (UPPERCASE)
    🏔️  X-Small warehouse optimization
    📊 Intelligent batch sizing based on dataset size
    🔄 Progress tracking and detailed logging
    
    # Generate STG files with default configuration
    python main.py generate-stg
    
    # Generate STG files with custom database and schema
    python main.py generate-stg --database DW_REPORTS --schema TEMP_ARGO_RAW
    
    # Dry run - see what would be generated without creating files
    python main.py generate-stg --dry-run
    
    # Read-only mode - don't update Check column in Google Sheets
    python main.py generate-stg --read-only
    
    # Generate dbt sources.yml with default configuration
    python main.py generate-sources
    
    # Generate sources.yml with custom database and schema
    python main.py generate-sources --database DW_RAW --schema SRC
    
    # Generate sources.yml with custom output file
    python main.py generate-sources --database DW_RAW --schema SRC --output my_sources.yml
    
Environment Variables:
    EXPORT_DIR: Default export directory
    GOOGLE_SHEETS_CREDENTIALS_FILE: Path to Google Sheets credentials file
    MIGRATION_SPREADSHEET_ID: Default spreadsheet ID for migrations and inventory
    COMPARISON_SPREADSHEET_ID: Default spreadsheet ID for comparisons
    MIGRATION_SHEET_NAME: Default sheet name for migrations (default: Migration)
    COMPARISON_SHEET_NAME: Default sheet name for comparisons (default: QA - Test)
    INTERMEDIATE_MODELS_SHEET_NAME: Default sheet name for inventory (default: Inventory)
    DOMO_DEVELOPER_TOKEN: Domo API developer token
    DOMO_INSTANCE: Domo instance name
    SNOWFLAKE_ACCOUNT: Snowflake account identifier
    SNOWFLAKE_USER: Snowflake username
    SNOWFLAKE_PASSWORD: Snowflake password
    SNOWFLAKE_WAREHOUSE: Snowflake warehouse name
    SNOWFLAKE_DATABASE: Snowflake database name
    SNOWFLAKE_SCHEMA: Snowflake schema name
    SNOWFLAKE_ROLE: Snowflake role name (optional, for privilege management)
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
        default=os.getenv("EXPORT_DIR", "results/sql/translated"),
        help="Directory to save SQL files (default: results/sql/translated)"
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
        "--from-spreadsheet",
        action="store_true",
        help="Migrate datasets from Google Sheets Migration tab"
    )
    
    migrate_parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    migrate_parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("MIGRATION_SPREADSHEET_ID"),
        help="Google Sheets spreadsheet ID (uses default if not specified)"
    )
    
    migrate_parser.add_argument(
        "--sheet-name",
        default=os.getenv("MIGRATION_SHEET_NAME", "Migration"),
        help="Migration sheet tab name (default: Migration)"
    )
    
    migrate_parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test Domo and Snowflake connections"
    )
    
    migrate_parser.add_argument(
        "--reload-env",
        action="store_true",
        help="Force reload environment variables from .env file"
    )
    
    migrate_parser.add_argument(
        "--full-table",
        action="store_true",
        help="Upload the entire table instead of limiting to first 1000 rows (default: False, limits to 1000 rows)"
    )
    
    migrate_parser.add_argument(
        "--auto-chunk-size",
        action="store_true",
        help="Automatically determine optimal chunk size for X-Small warehouse based on dataset size (default: False, uses fixed 1000 row chunks)"
    )
    
    # Datasets subcommand
    datasets_parser = subparsers.add_parser(
        'datasets',
        help='Manage Domo datasets'
    )
    
    datasets_parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test Domo connection"
    )
    
    datasets_parser.add_argument(
        "--export-to-spreadsheet",
        action="store_true",
        help="Export all Domo datasets to a Google Sheets spreadsheet"
    )
    
    datasets_parser.add_argument(
        "--list-local",
        action="store_true",
        help="List all Domo datasets locally"
    )
    
    datasets_parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    datasets_parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("MIGRATION_SPREADSHEET_ID"),
        help="Google Sheets spreadsheet ID to export to (uses default if not specified)"
    )
    
    datasets_parser.add_argument(
        "--sheet-name",
        default=os.getenv("DATASETS_SHEET_NAME", "Datasets"),
        help="Sheet name for Domo datasets (default: DomoDatasets)"
    )
    
    datasets_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of datasets to fetch per batch (default: 100)"
    )
    
    # Compare subcommand
    compare_parser = subparsers.add_parser(
        'compare',
        help='Compare a Domo dataset with a Snowflake table'
    )
    
    compare_parser.add_argument(
        "--domo-dataset-id",
        help="Domo output ID to compare"
    )
    
    compare_parser.add_argument(
        "--snowflake-table",
        help="Snowflake table name to compare"
    )
    
    compare_parser.add_argument(
        "--key-columns",
        nargs='+',
        help="One or more key columns to use for comparison"
    )
    
    compare_parser.add_argument(
        "--sample-size",
        type=int,
        help="Number of rows to sample for comparison (default: automatic calculation)"
    )
    
    compare_parser.add_argument(
        "--transform-columns",
        action="store_true",
        help="Transform column names for comparison (e.g., 'My Column' -> 'my_column')"
    )
    
    compare_parser.add_argument(
        "--sampling-method",
        choices=["random", "ordered"],
        default="random",
        help="Sampling method: 'random' (tries smart random with fallback to ordered) or 'ordered' (direct ordered sampling). Default: random"
    )
    
    compare_parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test Domo and Snowflake connections for comparison"
    )
    
    compare_parser.add_argument(
        "--from-spreadsheet",
        action="store_true",
        help="Compare datasets from Google Sheets Comparison tab"
    )
    
    compare_parser.add_argument(
        "--from-inventory",
        action="store_true",
        help="Compare datasets from existing Inventory spreadsheet (uses Output ID, Model Name, Key Columns)"
    )
    
    compare_parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    compare_parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("MIGRATION_SPREADSHEET_ID"),
        help="Google Sheets spreadsheet ID for comparisons (uses default if not specified)"
    )
    
    compare_parser.add_argument(
        "--sheet-name",
        default=os.getenv("COMPARISON_SHEET_NAME", "QA - Test"),
        help="Comparison sheet tab name (default: QA - Test)"
    )
    
    # Generate STG subcommand
    generate_stg_parser = subparsers.add_parser(
        'generate-stg',
        help='Generate staging SQL files from Google Sheets with Snowflake schema validation'
    )
    
    # Snowflake configuration
    generate_stg_parser.add_argument(
        "--database", 
        default=os.getenv("SNOWFLAKE_DATABASE"),
        help="Snowflake database name (default: from SNOWFLAKE_DATABASE env var)"
    )
    
    generate_stg_parser.add_argument(
        "--schema", 
        default="TEMP_ARGO_RAW",
        help="Snowflake schema name (default: TEMP_ARGO_RAW)"
    )
    
    generate_stg_parser.add_argument(
        "--role", 
        default="DBT_ROLE",
        help="Snowflake role to use (default: DBT_ROLE)"
    )
    
    generate_stg_parser.add_argument(
        "--warehouse", 
        default=os.getenv("SNOWFLAKE_WAREHOUSE"),
        help="Snowflake warehouse to use (default: from SNOWFLAKE_WAREHOUSE env var)"
    )
    
    generate_stg_parser.add_argument(
        "--output-dir", 
        default="sql/stg/",
        help="Directory to save SQL files (default: sql/stg/)"
    )
    
    # Google Sheets configuration
    generate_stg_parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    generate_stg_parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("MIGRATION_SPREADSHEET_ID"),
        help="Google Sheets spreadsheet ID"
    )
    
    # Options
    generate_stg_parser.add_argument(
        "--read-only",
        action="store_true",
        help="Run in read-only mode (don't update Check column in Google Sheets)"
    )
    
    generate_stg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without creating files or updating sheets"
    )

    # Generate Sources subcommand
    generate_sources_parser = subparsers.add_parser(
        'generate-sources',
        help='Generate dbt sources.yml file from Google Sheets data'
    )
    
    generate_sources_parser.add_argument(
        "--database", 
        default=os.getenv("SNOWFLAKE_DATABASE"),
        help="Snowflake database name (default: from SNOWFLAKE_DATABASE env var)"
    )
    
    generate_sources_parser.add_argument(
        "--schema", 
        default="SRC",
        help="Snowflake schema name (default: SRC)"
    )
    
    generate_sources_parser.add_argument(
        "--output", 
        default="sources_auto.yml",
        help="Output file name (default: sources_auto.yml)"
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
    elif args.command == 'datasets':
        return handle_datasets_command(args)
    elif args.command == 'compare':
        return handle_compare_command(args)
    elif args.command == 'generate-stg':
        return handle_generate_stg_command(args)
    elif args.command == 'generate-sources':
        return handle_generate_sources_command(args)
    
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