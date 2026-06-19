"""Command handlers for the CLI. ``main`` only parses args and dispatches here.

Each ``handle_*`` returns an exit code (0 = success, 1 = failure).
"""

import os
import json
import logging
from typing import Optional

from tools.inventory_handler import export_dataflows_to_sql, export_dataflows_raw, InventoryHandler
from tools.domo_to_snowflake import (
    migrate_dataset, migrate_dataset_to_stage, load_from_stage_to_table,
    migrate_dataset_via_stage, batch_migrate_datasets, migrate_from_spreadsheet,
    migrate_from_spreadsheet_to_stage, MigrationManager,
)
from tools.utils import DomoHandler, SnowflakeHandler, show_mfa_debug_info, reload_environment
from tools.utils.domo import export_datasets_to_spreadsheet, export_dataflows_to_spreadsheet, count_cards_to_spreadsheet
from tools.get_all_stg_files import (
    get_stg_files_data, generate_stg_files_from_dataframe,
    MODEL_NAME_COLUMN, OUTPUT_NAME_COLUMN, STATUS_COLUMN, DEPLOYED_STATUS,
)
from tools.utils.create_source import generate_sources_from_spreadsheet

logger = logging.getLogger(__name__)

DATAFLOW_COLUMN_NAMES = ["Dataflow ID", "dataflow", "Dataflow", "DataFlow", "dataflow_id", "Dataflow_ID"]


def _make_dataset_comparator():
    """Lazy import: only the compare command needs datacompy."""
    from tools.dataset_comparator import DatasetComparator
    return DatasetComparator()


def _resolve_chunk_size(args):
    """Map --auto-chunk-size / --full-table to a chunk_size ("auto" / None / 1000)."""
    if args.auto_chunk_size:
        logger.info("📊 X-Small optimized auto-chunk mode enabled")
        return "auto"
    if args.full_table:
        logger.info("📊 Full table mode: Will upload entire dataset")
        return None
    logger.info("📊 Limited mode: Will upload first 1000 rows")
    return 1000


def test_inventory_connection(credentials_path: Optional[str] = None) -> bool:
    """Test the Google Sheets connection and show an inventory preview."""
    try:
        logger.info("🧪 Testing Google Sheets connection...")
        credentials_path = credentials_path or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        if not credentials_path:
            logger.error("❌ No credentials file specified")
            logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
            return False

        extractor = InventoryHandler(credentials_path=credentials_path)
        df = extractor.get_inventory()
        logger.info("✅ Connection successful!")
        logger.info("📊 Inventory preview (first 5 rows):")
        print(df.head())

        dataflow_column = next((c for c in DATAFLOW_COLUMN_NAMES if c in df.columns), None)
        if dataflow_column:
            logger.info(f"✅ Found dataflow column: '{dataflow_column}'")
            unique_dataflows = extractor.get_unique_dataflows(df, dataflow_column=dataflow_column)
            logger.info(f"📋 Found {len(unique_dataflows)} dataflows: {unique_dataflows[:10]}...")
        else:
            logger.warning(f"⚠️  No dataflow column found. Available columns: {list(df.columns)}")
        return True
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


def test_migration_connections() -> bool:
    """Test the Domo and Snowflake connections via MigrationManager."""
    try:
        logger.info("🧪 Testing migration connections...")
        show_mfa_debug_info()
        with MigrationManager():
            logger.info("✅ All migration connections tested successfully!")
            return True
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


def handle_inventory_command(args) -> int:
    """Extract inventory from Google Sheets and export dataflows to SQL."""
    credentials_path = args.credentials or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not credentials_path:
        logger.error("❌ Credentials file not specified")
        logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials")
        return 1
    if not os.path.exists(credentials_path):
        logger.error(f"❌ Credentials file not found: {credentials_path}")
        return 1

    if args.test_connection:
        return 0 if test_inventory_connection(credentials_path) else 1

    logger.info("🚀 Starting inventory export...")
    logger.info(f"📁 Export directory: {args.export_dir}")
    logger.info(f"🔑 Credentials file: {credentials_path}")
    if export_dataflows_to_sql(output_dir=args.export_dir, credentials_path=credentials_path):
        logger.info("🎉 Export completed successfully!")
        return 0
    logger.error("❌ Export failed!")
    return 1


def handle_dataflow_raw_command(args) -> int:
    """Export RAW Domo dataflow definitions (tiles/steps) as JSON, before translation."""
    dataflow_id = getattr(args, "dataflow_id", None)

    # Credentials are only needed when resolving dataflow IDs from the inventory sheet.
    credentials_path = args.credentials or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
    if not dataflow_id:
        if not credentials_path:
            logger.error("❌ Credentials file not specified")
            logger.error("Set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable or use --credentials (or pass --dataflow-id)")
            return 1
        if not os.path.exists(credentials_path):
            logger.error(f"❌ Credentials file not found: {credentials_path}")
            return 1

    logger.info("🚀 Starting raw dataflow export...")
    logger.info(f"📁 Output directory: {args.output_dir}")
    try:
        ok = export_dataflows_raw(
            output_dir=args.output_dir,
            credentials_path=credentials_path,
            dataflow_id=dataflow_id,
        )
    except (ImportError, ValueError) as e:
        logger.error(f"❌ {e}")
        return 1

    if ok:
        logger.info("🎉 Raw export completed successfully!")
        return 0
    logger.error("❌ Raw export failed!")
    return 1


def handle_migrate_command(args) -> int:
    """Migrate datasets Domo → Snowflake (single, batch, via-stage or spreadsheet)."""
    if args.reload_env:
        reload_environment()

    if args.test_connection:
        return 0 if test_migration_connections() else 1

    # Single dataset → stage
    if args.to_stage and args.dataset_id and args.stage_name:
        logger.info("🚀 Starting migration to stage...")
        logger.info(f"📊 Dataset ID: {args.dataset_id}")
        logger.info(f"🏗️  Stage name: {args.stage_name}")
        chunk_size = _resolve_chunk_size(args)
        show_mfa_debug_info()
        if migrate_dataset_to_stage(dataset_id=args.dataset_id, stage_name=args.stage_name, chunk_size=chunk_size):
            logger.info("✅ Migration to stage completed successfully")
            return 0
        logger.error("❌ Migration to stage failed")
        return 1

    # Stage → table
    if args.from_stage and args.stage_name and args.target_table:
        logger.info("🚀 Starting load from stage to table...")
        logger.info(f"🏗️  Stage name: {args.stage_name}")
        logger.info(f"📊 Target table: {args.target_table}")
        logger.info(f"📁 File pattern: {args.file_pattern}")
        logger.info(f"🔄 If exists: {args.if_exists}")
        show_mfa_debug_info()
        if load_from_stage_to_table(stage_name=args.stage_name, target_table=args.target_table,
                                    file_pattern=args.file_pattern, if_exists=args.if_exists):
            logger.info("✅ Load from stage to table completed successfully")
            return 0
        logger.error("❌ Load from stage to table failed")
        return 1

    # Domo → stage → table
    if args.dataset_id and args.stage_name and args.target_table:
        logger.info("🚀 Starting complete migration via stage...")
        logger.info(f"📊 Dataset ID: {args.dataset_id}")
        logger.info(f"🏗️  Stage name: {args.stage_name}")
        logger.info(f"📊 Target table: {args.target_table}")
        chunk_size = _resolve_chunk_size(args)
        logger.info(f"🔄 If exists: {args.if_exists}")
        show_mfa_debug_info()
        if migrate_dataset_via_stage(dataset_id=args.dataset_id, stage_name=args.stage_name,
                                     target_table=args.target_table, chunk_size=chunk_size, if_exists=args.if_exists):
            logger.info("✅ Complete migration via stage completed successfully")
            return 0
        logger.error("❌ Complete migration via stage failed")
        return 1

    # Spreadsheet-driven migration (optionally to stage)
    if args.from_spreadsheet:
        to_stage = args.to_stage and args.stage_name
        logger.info("🚀 Starting spreadsheet-based migration to stage..." if to_stage
                    else "🚀 Starting spreadsheet-based migration...")
        logger.info(f"📋 Spreadsheet ID: {args.spreadsheet_id}")
        logger.info(f"📄 Sheet name: {args.sheet_name}")
        if to_stage:
            logger.info(f"🏗️  Stage name: {args.stage_name}")
        if args.full_table:
            logger.info("📊 Full table mode: Will upload entire datasets (no row limit)")
        elif args.auto_chunk_size:
            logger.info("📊 X-Small optimized auto-chunk mode: Will automatically determine optimal chunk size based on dataset size for X-Small warehouse")
        else:
            logger.info("📊 Limited mode: Will upload first 1000 rows per dataset")
        logger.info("🔧 Column normalization: Automatic Snowflake compatibility (UPPERCASE)")
        show_mfa_debug_info()

        if to_stage:
            results = migrate_from_spreadsheet_to_stage(
                spreadsheet_id=args.spreadsheet_id, sheet_name=args.sheet_name, stage_name=args.stage_name,
                credentials_path=args.credentials, full_table=args.full_table, auto_chunk_size=args.auto_chunk_size)
        else:
            results = migrate_from_spreadsheet(
                spreadsheet_id=args.spreadsheet_id, sheet_name=args.sheet_name,
                credentials_path=args.credentials, full_table=args.full_table, auto_chunk_size=args.auto_chunk_size)

        if results.get('errors'):
            logger.error("❌ Spreadsheet migration failed due to errors:")
            for error in results['errors']:
                logger.error(f"   - {error}")
            return 1
        if results['failed'] == 0:
            logger.info("🎉 Spreadsheet migration completed successfully!")
            return 0
        logger.error(f"❌ Spreadsheet migration completed with {results['failed']} failures!")
        return 1

    # Single dataset → table
    if args.dataset_id and args.target_table:
        logger.info("🚀 Starting single dataset migration...")
        logger.info(f"📊 Dataset ID: {args.dataset_id}")
        logger.info(f"🎯 Target table: {args.target_table}")
        show_mfa_debug_info()
        if migrate_dataset(args.dataset_id, args.target_table):
            logger.info("🎉 Migration completed successfully!")
            return 0
        logger.error("❌ Migration failed!")
        return 1

    # Batch migration from a JSON mapping file
    if args.batch_file:
        logger.info("🚀 Starting batch migration...")
        logger.info(f"📁 Batch file: {args.batch_file}")
        show_mfa_debug_info()
        try:
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
            logger.error(f"❌ Batch migration completed with {results['failed']} failures!")
            return 1
        except Exception as e:
            logger.error(f"❌ Batch migration failed: {e}")
            return 1

    logger.error("❌ No valid migration options provided")
    logger.error("Use --dataset-id and --target-table for single migration, --batch-file for batch migration, or --from-spreadsheet for spreadsheet migration")
    return 1


def handle_datasets_command(args) -> int:
    """Manage Domo datasets: test connection, export to Sheets, or list locally."""
    if args.test_connection:
        logger.info("🧪 Testing Domo connection...")
        try:
            if DomoHandler().setup_auth():
                logger.info("✅ Domo connection successful")
                return 0
            logger.error("❌ Domo connection failed")
            return 1
        except Exception as e:
            logger.error(f"❌ Domo connection test failed: {e}")
            return 1

    if args.export_to_spreadsheet:
        logger.info("🚀 Starting dataset export to spreadsheet...")
        logger.info(f"📋 Spreadsheet ID: {args.spreadsheet_id}")
        logger.info(f"📄 Sheet name: {args.sheet_name}")
        if export_datasets_to_spreadsheet(spreadsheet_id=args.spreadsheet_id, sheet_name=args.sheet_name,
                                          credentials_path=args.credentials):
            logger.info("🎉 Dataset export completed successfully!")
            return 0
        logger.error("❌ Dataset export failed!")
        return 1

    if args.export_dataflows:
        logger.info("🚀 Starting dataflow lineage export to spreadsheet...")
        logger.info(f"📋 Spreadsheet ID: {args.spreadsheet_id}")
        if export_dataflows_to_spreadsheet(spreadsheet_id=args.spreadsheet_id,
                                           credentials_path=args.credentials,
                                           datasets_sheet_name=args.sheet_name):
            logger.info("🎉 Dataflow export completed successfully!")
            return 0
        logger.error("❌ Dataflow export failed!")
        return 1

    if args.count_cards:
        logger.info("🃏 Counting Domo cards per dataset...")
        logger.info(f"📋 Spreadsheet ID: {args.spreadsheet_id}")
        if count_cards_to_spreadsheet(spreadsheet_id=args.spreadsheet_id,
                                      sheet_name=args.sheet_name,
                                      credentials_path=args.credentials):
            logger.info("🎉 Card count completed successfully!")
            return 0
        logger.error("❌ Card count failed!")
        return 1

    if args.list_local:
        logger.info("📋 Fetching all datasets from Domo...")
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

    logger.error("❌ No valid dataset options provided")
    logger.error("Use --export-to-spreadsheet to export datasets to Google Sheets, --export-dataflows to export dataflow lineage, --count-cards to write a '# Cards' column, --list-local to list locally, or --test-connection to test Domo connection")
    return 1


def handle_compare_command(args) -> int:
    """Compare a Domo dataset with a Snowflake table or CSV file."""
    if args.from_spreadsheet:
        return handle_compare_from_spreadsheet(args)
    if args.from_inventory:
        return handle_compare_from_inventory(args)

    if not args.domo_dataset_id:
        logger.error("❌ Domo dataset ID is required")
        logger.error("Use --domo-dataset-id to specify the dataset to compare")
        return 1

    if args.csv_file:
        if not args.key_columns:
            logger.error("❌ Key columns are required for CSV comparison")
            logger.error("Use --key-columns to specify one or more key columns")
            return 1
        if not os.path.exists(args.csv_file):
            logger.error(f"❌ CSV file not found: {args.csv_file}")
            return 1
    else:
        if not args.snowflake_table:
            logger.error("❌ Snowflake table name is required")
            logger.error("Use --snowflake-table to specify the table to compare")
            logger.error("Or use --csv-file to compare with a CSV file")
            return 1
        if not args.key_columns:
            logger.error("❌ Key columns are required for comparison")
            logger.error("Use --key-columns to specify one or more key columns")
            return 1

    if args.test_connection:
        logger.info("🧪 Testing connections for comparison...")
        try:
            comparator = _make_dataset_comparator()
            if comparator.setup_connections():
                logger.info("✅ All connections for comparison tested successfully!")
                return 0
            logger.error("❌ Connection test failed!")
            return 1
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return 1
        finally:
            try:
                comparator.cleanup()
            except Exception:
                pass

    if args.csv_file:
        logger.info("🚀 Starting Domo vs CSV comparison...")
        logger.info(f"📊 Domo Dataset ID: {args.domo_dataset_id}")
        logger.info(f"📁 CSV File: {args.csv_file}")
        logger.info(f"🔑 Key Columns: {', '.join(args.key_columns)}")
        logger.info(f"📝 CSV Encoding: {args.csv_encoding}")
        logger.info(f"📝 CSV Separator: '{args.csv_separator}'")
    else:
        logger.info("🚀 Starting Domo vs Snowflake comparison...")
        logger.info(f"📊 Domo Dataset ID: {args.domo_dataset_id}")
        logger.info(f"❄️  Snowflake Table: {args.snowflake_table}")
        logger.info(f"🔑 Key Columns: {', '.join(args.key_columns)}")

    logger.info(f"📏 Sample Size: {args.sample_size:,}" if args.sample_size else "📏 Sample Size: Automatic calculation")
    logger.info("🔄 Column Name Transformation: Enabled" if args.transform_columns else "🔄 Column Name Transformation: Disabled")
    logger.info(f"🎲 Sampling Method: {args.sampling_method}")

    try:
        comparator = _make_dataset_comparator()
        if args.csv_file:
            if not comparator.domo_handler.setup_auth():
                logger.error("❌ Failed to setup Domo connection")
                return 1
            report = comparator.compare_with_csv(
                domo_dataset_id=args.domo_dataset_id, csv_file_path=args.csv_file, key_columns=args.key_columns,
                sample_size=args.sample_size, transform_names=args.transform_columns,
                sampling_method=args.sampling_method, csv_encoding=args.csv_encoding, csv_separator=args.csv_separator)
            comparator.print_csv_report(report)
        else:
            if not comparator.setup_connections():
                logger.error("❌ Failed to setup connections")
                return 1
            report = comparator.generate_report(
                domo_dataset_id=args.domo_dataset_id, snowflake_table=args.snowflake_table,
                key_columns=args.key_columns, sample_size=args.sample_size,
                transform_names=args.transform_columns, sampling_method=args.sampling_method)
            comparator.print_report(report)

        if report.get('errors'):
            logger.error("❌ Comparison completed with errors")
            return 1
        if report.get('overall_match', False):
            logger.info("🎉 Comparison completed successfully - datasets match!")
            return 0
        logger.warning("⚠️  Comparison completed - discrepancies found")
        return 0
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
        except Exception:
            pass


def handle_compare_from_spreadsheet(args) -> int:
    """Run comparisons for every row of a Google Sheets comparison tab."""
    logger.info("🚀 Starting spreadsheet-based comparisons...")
    from tools.utils.common import get_env_config
    spreadsheet_id = args.spreadsheet_id or get_env_config().get("MIGRATION_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.error("❌ Spreadsheet ID is required for spreadsheet-based comparisons")
        logger.error("Set MIGRATION_SPREADSHEET_ID environment variable or use --spreadsheet-id")
        return 1

    try:
        comparator = _make_dataset_comparator()
        results = comparator.compare_from_spreadsheet(
            spreadsheet_id=spreadsheet_id, sheet_name=args.sheet_name, credentials_path=args.credentials,
            sampling_method=args.sampling_method, use_schema=args.use_schema)
        return _report_batch_comparison(results, "spreadsheet")
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
        except Exception:
            pass


def handle_compare_from_inventory(args) -> int:
    """Run comparisons driven by the existing Inventory spreadsheet."""
    logger.info("🚀 Starting inventory-based comparisons...")
    logger.info("📋 Using existing inventory spreadsheet configuration")
    try:
        comparator = _make_dataset_comparator()
        results = comparator.compare_from_inventory(
            credentials_path=args.credentials, sampling_method=args.sampling_method)
        return _report_batch_comparison(results, "inventory")
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
        except Exception:
            pass


def _report_batch_comparison(results: dict, source: str) -> int:
    """Shared exit-code/logging logic for the spreadsheet and inventory comparisons."""
    if results.get('errors'):
        logger.error(f"❌ {source.capitalize()} comparison failed due to errors:")
        for error in results['errors'][:5]:
            logger.error(f"   - {error}")
        if len(results['errors']) > 5:
            logger.error(f"   ... and {len(results['errors']) - 5} more errors")
        return 1
    if results['failed'] == 0:
        logger.info(f"🎉 All {source} comparisons completed successfully!")
        return 0
    logger.warning(f"⚠️  {source.capitalize()} comparisons completed with {results['failed']} failures!")
    return 0


def handle_stage_command(args) -> int:
    """Manage Snowflake stages (create / list / drop / clean)."""
    if not args.stage_action:
        logger.error("❌ No stage action specified. Use 'create', 'list', 'drop', or 'clean'")
        return 1
    if not test_migration_connections():
        logger.error("❌ Connection test failed")
        return 1

    try:
        with MigrationManager() as manager:
            stage = args.stage_name
            if args.stage_action == 'create':
                logger.info(f"🏗️  Creating stage: {stage}")
                if manager.stage_handler.create_stage(stage):
                    logger.info(f"✅ Stage '{stage}' created successfully")
                    return 0
                logger.error(f"❌ Failed to create stage '{stage}'")
                return 1
            if args.stage_action == 'list':
                logger.info(f"📁 Listing files in stage: {stage}")
                files = manager.stage_handler.list_stage_files(stage)
                if files is None:
                    logger.error(f"❌ Failed to list files in stage '{stage}'")
                    return 1
                if files:
                    logger.info(f"📁 Found {len(files)} file(s) in stage '{stage}':")
                    for file_info in files:
                        logger.info(f"   - {file_info['name']} ({file_info['size']} bytes, {file_info['last_modified']})")
                else:
                    logger.info(f"📁 Stage '{stage}' is empty")
                return 0
            if args.stage_action == 'drop':
                logger.info(f"🗑️  Dropping stage: {stage}")
                if manager.stage_handler.drop_stage(stage):
                    logger.info(f"✅ Stage '{stage}' dropped successfully")
                    return 0
                logger.error(f"❌ Failed to drop stage '{stage}'")
                return 1
            if args.stage_action == 'clean':
                logger.info(f"🧹 Cleaning files from stage: {stage}")
                logger.info(f"📁 File pattern: {args.file_pattern}")
                if manager.stage_handler.remove_stage_files(stage, args.file_pattern):
                    logger.info(f"✅ Files cleaned from stage '{stage}'")
                    return 0
                logger.error(f"❌ Failed to clean files from stage '{stage}'")
                return 1
            logger.error(f"❌ Unknown stage action: {args.stage_action}")
            return 1
    except Exception as e:
        logger.error(f"❌ Stage operation failed: {e}")
        return 1


def handle_generate_stg_command(args) -> int:
    """Generate staging SQL files from the 'Staging models' tab with Snowflake schema validation."""
    if not args.database:
        logger.error("❌ Database not specified. Use --database or set SNOWFLAKE_DATABASE environment variable.")
        return 1
    if not args.credentials:
        logger.error("❌ Google Sheets credentials not specified. Use --credentials or set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable.")
        return 1
    if not args.spreadsheet_id:
        logger.error("❌ Spreadsheet ID not specified. Use --spreadsheet-id or set MIGRATION_SPREADSHEET_ID environment variable.")
        return 1

    logger.info("🚀 Starting staging files generation...")
    logger.info(f"📊 Database: {args.database}")
    logger.info(f"📂 Schema: {args.schema}")
    logger.info(f"👤 Role: {args.role}")
    logger.info(f"🏠 Warehouse: {args.warehouse}")
    logger.info(f"📁 Output: {args.output_dir}")
    logger.info(f"📄 Spreadsheet: {args.spreadsheet_id}")
    logger.info("🔧 CAST mode: Will use explicit CAST statements in SQL" if args.use_cast
                else "🔧 No CAST mode: Will generate SQL without explicit CAST statements (default)")
    if args.read_only:
        logger.info("⚠️  Read-only mode: Will not update the Status column")
    if args.dry_run:
        logger.info("🧪 Dry-run mode: Will not create files or update sheets")

    try:
        df, gsheets, spreadsheet_id = get_stg_files_data()
        if df.empty:
            logger.error("❌ Could not obtain data from spreadsheet.")
            return 1
        logger.info("✅ Data extracted successfully.")

        if gsheets and spreadsheet_id and not args.read_only:
            logger.info("✅ Google Sheets write permissions confirmed.")
        else:
            logger.info("⚠️  Google Sheets updates disabled.")
            gsheets = None

        if args.dry_run:
            logger.info("🧪 Dry-run mode - showing what would be processed:")
            status_norm = df[STATUS_COLUMN].astype(str).str.strip().str.lower()
            pending_rows = df[status_norm != DEPLOYED_STATUS.lower()]
            completed_rows = df[status_norm == DEPLOYED_STATUS.lower()]
            logger.info(f"   ✅ Already deployed: {len(completed_rows)} files")
            logger.info(f"   🔄 Would process: {len(pending_rows)} files")
            if not pending_rows.empty:
                logger.info("📋 Files that would be generated:")
                for _, row in pending_rows.head(10).iterrows():
                    logger.info(f"   • {row[MODEL_NAME_COLUMN]} (from {row[OUTPUT_NAME_COLUMN]})")
                if len(pending_rows) > 10:
                    logger.info(f"   ... and {len(pending_rows) - 10} more files")
            logger.info("🧪 Dry-run completed. Use without --dry-run to actually generate files.")
            return 0

        logger.info("🔄 Starting automatic SQL file generation...")
        generate_stg_files_from_dataframe(
            df=df, database=args.database, schema=args.schema, output_dir=args.output_dir,
            role=args.role, warehouse=args.warehouse, gsheets=gsheets,
            spreadsheet_id=spreadsheet_id, use_cast=args.use_cast)
        logger.info("🎉 Process completed successfully!")
        return 0
    except KeyboardInterrupt:
        logger.info("⚠️  STG generation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ STG generation failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify that the spreadsheet ID and credentials are correct")
        logger.error("   - Check that the 'Staging models' tab exists in the spreadsheet")
        logger.error("   - Verify Snowflake connection credentials and permissions")
        logger.error("   - Ensure the database and schema exist in Snowflake")
        return 1


def handle_generate_sources_command(args) -> int:
    """Generate a dbt sources.yml file from Google Sheets data."""
    if not args.database:
        logger.error("❌ Database not specified. Use --database or set SNOWFLAKE_DATABASE environment variable.")
        return 1
    if not args.schema:
        logger.error("❌ Schema not specified. Use --schema.")
        return 1

    logger.info("🚀 Starting dbt sources.yml generation...")
    logger.info(f"📊 Database: {args.database}")
    logger.info(f"📂 Schema: {args.schema}")
    logger.info(f"📁 Output: {args.output}")
    try:
        if generate_sources_from_spreadsheet(database=args.database, schema=args.schema, output_file=args.output):
            logger.info("🎉 Sources file generated successfully!")
            return 0
        logger.error("❌ Sources generation failed!")
        return 1
    except KeyboardInterrupt:
        logger.info("⚠️  Sources generation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Sources generation failed: {e}")
        logger.error("💡 Suggestions:")
        logger.error("   - Verify Google Sheets credentials and spreadsheet access")
        logger.error("   - Check that the 'Staging models' tab exists in the spreadsheet")
        logger.error("   - Ensure the 'Output Name' column contains valid table names")
        return 1


def handle_weighting_command(args) -> int:
    """Forward to the translation-difficulty CLI (tools.utils.translation_difficulty)."""
    argv = list(getattr(args, "weighting_argv", None) or [])
    if argv and argv[0] == "":
        argv = argv[1:]
    try:
        from tools.utils.translation_difficulty.cli import main as td_main
    except ImportError as e:
        logger.error("Translation difficulty / weighting requires domo_utils (install argo-utils-cli). %s", e)
        return 1
    return td_main(argv)


# ── refresh: run the Domo → spreadsheet steps in dependency order ────────────
# Each step is (key, human description, callable(args) -> bool). Order matters:
# 'datasets' seeds the 'All Datasets' tab that 'cards' and 'dataflows' read.
def _refresh_step_datasets(args) -> bool:
    return export_datasets_to_spreadsheet(
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=os.getenv("DATASETS_SHEET_NAME", "All Datasets"),
        credentials_path=args.credentials,
    )


def _refresh_step_cards(args) -> bool:
    return count_cards_to_spreadsheet(
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=os.getenv("DATASETS_SHEET_NAME", "All Datasets"),
        credentials_path=args.credentials,
    )


def _refresh_step_dataflows(args) -> bool:
    return export_dataflows_to_spreadsheet(
        spreadsheet_id=args.spreadsheet_id,
        credentials_path=args.credentials,
    )


def _refresh_weighting(argv) -> bool:
    try:
        from tools.utils.translation_difficulty.cli import main as td_main
    except ImportError as e:
        logger.error("weighting steps require domo_utils (install argo-utils-cli). %s", e)
        return False
    return td_main(argv) == 0


def _refresh_step_inventory(args) -> bool:
    return _refresh_weighting(["export-inventory"])


def _refresh_step_score(args) -> bool:
    return _refresh_weighting(["score", "--from-api-list"])


# (key, description, fn, is_slow)
_REFRESH_STEPS = [
    ("datasets", "Export Domo datasets → 'All Datasets'", _refresh_step_datasets, False),
    ("cards", "Count cards per dataset → '# Cards'", _refresh_step_cards, False),
    ("dataflows", "Export dataflow lineage → 'All Dataflows'", _refresh_step_dataflows, False),
    ("inventory", "Export dataflow inventory → 'Dataflow Inventory'", _refresh_step_inventory, False),
    ("score", "Score translation difficulty → 'CTE Points Analysis' (SLOW, ~20 min)", _refresh_step_score, True),
]


def handle_refresh_command(args) -> int:
    """Run the Domo → spreadsheet pipeline steps in dependency order.

    By default runs every step EXCEPT the slow 'score' (opt in with --with-score).
    Continues past failures and prints a summary unless --fail-fast is set.
    """
    valid = [k for k, _d, _f, _s in _REFRESH_STEPS]

    # Resolve which steps to run.
    if args.only:
        requested = [s.strip() for s in args.only.split(",") if s.strip()]
        unknown = [s for s in requested if s not in valid]
        if unknown:
            logger.error("❌ Unknown step(s) in --only: %s. Valid: %s", unknown, valid)
            return 1
        selected = [k for k in valid if k in requested]
    else:
        # Default = all fast steps; 'score' only with --with-score.
        selected = [k for k, _d, _f, slow in _REFRESH_STEPS if not slow]
        if args.with_score:
            selected.append("score")

    if args.skip:
        skip = {s.strip() for s in args.skip.split(",") if s.strip()}
        selected = [k for k in selected if k not in skip]

    if not selected:
        logger.error("❌ No steps selected to run.")
        return 1

    steps = [(k, d, f) for (k, d, f, _s) in _REFRESH_STEPS if k in selected]

    logger.info("🔄 Refresh plan (%s step(s)):", len(steps))
    for i, (k, d, _f) in enumerate(steps, 1):
        logger.info("   %s. [%s] %s", i, k, d)

    if args.dry_run:
        logger.info("🧪 Dry run — nothing executed.")
        return 0

    results: list[tuple[str, bool]] = []
    for k, d, fn in steps:
        logger.info("━━━ ▶️  %s: %s", k, d)
        try:
            ok = bool(fn(args))
        except Exception as e:  # noqa: BLE001
            logger.error("❌ Step '%s' raised: %s", k, e)
            ok = False
        results.append((k, ok))
        logger.info("━━━ %s %s", "✅" if ok else "❌", k)
        if not ok and args.fail_fast:
            logger.error("🛑 --fail-fast: stopping after failed step '%s'", k)
            break

    ok_count = sum(1 for _k, ok in results if ok)
    logger.info("📋 Refresh summary: %s/%s succeeded", ok_count, len(results))
    for k, ok in results:
        logger.info("   %s %s", "✅" if ok else "❌", k)
    return 0 if ok_count == len(results) and len(results) == len(steps) else 1
