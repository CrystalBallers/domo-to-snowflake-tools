#!/usr/bin/env python3
"""
Generate staging SQL files from Google Sheets with Snowflake schema validation.

⚠️  DEPRECATION NOTICE: This standalone script is now integrated as a subcommand in main.py
📍 RECOMMENDED: Use `python main.py generate-stg [options]` instead

This script reads from the 'Stg Files' tab in Google Sheets and generates SQL files
with automatic CAST statements based on real Snowflake column types.

Legacy Usage (still supported):
    python tools/get_all_stg_files.py [options]

Recommended Usage:
    python main.py generate-stg [options]
    
Features:
    - Automatic CAST generation based on Snowflake data types
    - Smart skip: Only processes rows where Check != "True"  
    - Progress tracking: Updates Check column when files are created successfully
    - Schema validation: Connects to Snowflake to get real column names and types
"""

import os
import sys
import pandas as pd
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tools.utils.gsheets import GoogleSheets, READ_WRITE_SCOPES
from tools.utils.create_stg_sql_file import create_stg_sql_file
from tools.utils.snowflake import SnowflakeHandler

# Load environment variables
load_dotenv()

def get_stg_files_data():
    """
    Extracts data from the 'Stg Files' tab of the Google Spreadsheet.
    
    Returns:
        tuple: (pd.DataFrame, GoogleSheets client, spreadsheet_id) - Data and objects needed for writing back
    """
    try:
        # Get spreadsheet ID from environment variables
        spreadsheet_id = os.getenv("MIGRATION_SPREADSHEET_ID")
        if not spreadsheet_id:
            print("❌ Error: Environment variable MIGRATION_SPREADSHEET_ID is not configured.")
            return pd.DataFrame(), None, None
        
        # Get credentials path
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        if not credentials_path:
            print("❌ Error: Environment variable GOOGLE_SHEETS_CREDENTIALS_FILE is not configured.")
            return pd.DataFrame(), None, None
        
        # Initialize Google Sheets client with READ_WRITE permissions
        print("🔐 Initializing Google Sheets with write permissions...")
        gsheets = GoogleSheets(credentials_path=credentials_path, scopes=READ_WRITE_SCOPES)
        
        # Read data from 'Stg Files' tab
        print("📊 Reading data from 'Stg Files' tab...")
        df = gsheets.read_to_dataframe(spreadsheet_id, "Stg Files!A:Z", header=True)
        
        if df.is_empty():
            print("❌ No data found in 'Stg Files' tab.")
            return pd.DataFrame(), None, None
        
        # Convert from polars to pandas
        pandas_df = df.to_pandas()
        
        print(f"✅ Found {len(pandas_df)} datasets to process.")
        
        return pandas_df, gsheets, spreadsheet_id
        
    except Exception as e:
        print(f"❌ Error reading data from spreadsheet: {e}")
        return pd.DataFrame(), None, None


def generate_stg_files_from_dataframe(df: pd.DataFrame, database: str = None, schema: str = "TEMP_ARGO_RAW", output_dir: str = "sql/stg/", role: str = "DBT_ROLE", warehouse: str = None, gsheets=None, spreadsheet_id: str = None):
    """
    Iterates through each row of the DataFrame and generates staging SQL files.
    Gets real column names from Snowflake for each table.
    Writes "True" to the Check column when files are created successfully.
    
    Args:
        df: DataFrame with data from the 'Stg Files' tab
        database: Snowflake database name (if None, uses environment variable)
        schema: Snowflake schema name (default: "TEMP_ARGO_RAW")
        output_dir: Directory where to save the SQL files
        role: Snowflake role to use (default: "DBT_ROLE")
        warehouse: Snowflake warehouse to use (if None, uses environment variable)
        gsheets: GoogleSheets client for writing back to spreadsheet
        spreadsheet_id: ID of the Google Spreadsheet
    """
    if df.empty:
        print("❌ DataFrame is empty. Cannot generate files.")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get database from environment if not provided
    if database is None:
        database = os.getenv("SNOWFLAKE_DATABASE")
        if not database:
            print("❌ Database not provided and SNOWFLAKE_DATABASE environment variable not set.")
            return
    
    # Initialize Snowflake connection to get real column names
    print("🔗 Connecting to Snowflake to get table schemas...")
    snowflake_handler = SnowflakeHandler()
    
    if not snowflake_handler.setup_connection():
        print("❌ Failed to connect to Snowflake. Using placeholder columns.")
        snowflake_handler = None
    else:
        print("✅ Connected to Snowflake successfully.")
    
    print(f"\n🔄 Generating SQL files for {len(df)} datasets...")
    print("=" * 80)
    
    generated_count = 0
    skipped_count = 0
    already_completed_count = 0
    
    # Iterate through each row of the DataFrame
    for index, row in df.iterrows():
        dataset_id = row['Dataset ID']
        check_status = row['Check']
        name = row['Name']
        model_filename = row['Model']
        
        # Skip rows that already have Check = True
        if str(check_status).lower() == 'true':
            print(f"✅ Row {index + 1}: Already completed ({name}) - skipping...")
            already_completed_count += 1
            continue
        
        # Verify that necessary data is present
        if pd.isna(name) or pd.isna(model_filename):
            print(f"⚠️  Row {index + 1}: Incomplete data - skipping...")
            skipped_count += 1
            continue
        
        # Create the full file path
        output_path = os.path.join(output_dir, model_filename)
        
        print(f"\n📝 Processing row {index + 1}:")
        print(f"   Dataset ID: {dataset_id}")
        print(f"   Check: {check_status}")
        print(f"   Name: {name}")
        print(f"   Model: {model_filename}")
        
        # Get real column names from Snowflake
        columns = None
        error_message = None
        
        if snowflake_handler:
            print(f"   🔍 Getting columns from Snowflake table: {database}.{schema}.{name} using role: {role}")
            columns = snowflake_handler.get_table_columns(database, schema, name, role, warehouse)
            
            if not columns:
                print(f"   ❌ Failed to get columns from Snowflake table: {database}.{schema}.{name}")
                print(f"   💡 Table may not exist or you may not have permissions with role: {role}")
                error_message = f"ERROR: Could not retrieve columns for table {database}.{schema}.{name} using role: {role}"
            else:
                print(f"   ✅ Found {len(columns)} columns with data types from Snowflake")
                # Show first few columns with types for verification
                sample_cols = columns[:3] if len(columns) > 3 else columns
                for col in sample_cols:
                    print(f"      • {col['name']} ({col['data_type']})")
                if len(columns) > 3:
                    print(f"      ... and {len(columns) - 3} more columns")
        else:
            print(f"   ❌ No Snowflake connection available")
            error_message = "ERROR: No Snowflake connection available to retrieve table schema"
        
        try:
            if error_message:
                # Create file with error message instead of SQL
                print(f"   ⚠️  Creating file with error message: {model_filename}")
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(f"""-- {error_message}
-- 
-- This file could not be generated because the table schema could not be retrieved.
-- 
-- Possible causes:
-- 1. Table {database}.{schema}.{name} does not exist
-- 2. Insufficient permissions to access the table with role: {role}
-- 3. Snowflake connection failed
-- 4. Warehouse not available or not specified
-- 5. Role {role} does not have access to the table
-- 
-- Please verify the table exists and you have proper permissions with role: {role}, then regenerate this file.

/*
TABLE: {database}.{schema}.{name}
DATASET_ID: {dataset_id}
ROLE_USED: {role}
GENERATED_AT: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
STATUS: FAILED
*/

-- Uncomment and modify the following template when the table is available:
/*
with
    source as (select * from {{{{ source("{schema}", "{name}") }}}})

select
    -- Add your columns here when table schema is available
    *

from source
*/
""")
                print(f"   ⚠️  Error file created: {output_path}")
                skipped_count += 1
            else:
                # Call the create_stg_sql_file function with real columns
                sql_content = create_stg_sql_file(
                    columns=columns,
                    source_schema_name=schema,
                    source_table_name=name,
                    output_filename=output_path
                )
                print(f"   ✅ File generated: {output_path}")
                generated_count += 1
                
                # Write "True" back to the Check column in Google Sheets
                if gsheets and spreadsheet_id:
                    try:
                        # Calculate the cell reference for the Check column (assuming it's column B, row is index+2 due to header)
                        check_cell = f"Stg Files!B{index + 2}"
                        gsheets.write_range(spreadsheet_id, check_cell, [["True"]])
                        print(f"   📝 Updated Check column to 'True' for row {index + 1}")
                    except Exception as write_error:
                        print(f"   ⚠️  Warning: Could not update Check column: {write_error}")
                        # Don't fail the whole process if we can't write back
            
        except Exception as e:
            print(f"   ❌ Error generating file: {e}")
            skipped_count += 1
    
    # Cleanup Snowflake connection
    if snowflake_handler:
        snowflake_handler.cleanup()
        print("🔌 Snowflake connection closed.")
    
    print("\n" + "=" * 80)
    print(f"📊 Generation summary:")
    print(f"   ✅ Files generated: {generated_count}")
    print(f"   ✅ Already completed: {already_completed_count}")
    print(f"   ⚠️  Files skipped: {skipped_count}")
    print(f"   📁 Output directory: {output_dir}")
    print(f"   🗄️  Database used: {database}")
    print(f"   📂 Schema used: {schema}")
    print(f"   👤 Role used: {role}")
    if gsheets and spreadsheet_id:
        print(f"   📝 Google Sheets updates: Enabled")
    else:
        print(f"   📝 Google Sheets updates: Disabled (read-only mode)")
    print("=" * 80)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate staging SQL files from Google Sheets data with Snowflake schema validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Use default configuration from environment variables
    python tools/get_all_stg_files.py
    
    # Override specific settings
    python tools/get_all_stg_files.py --database DW_REPORTS --schema TEMP_ARGO_RAW
    
    # Full custom configuration
    python tools/get_all_stg_files.py --database DW_RAW --schema SRC --role DBT_ROLE --warehouse DBT_WH --output-dir results/sql/stg
    
Environment Variables:
    SNOWFLAKE_DATABASE: Default database (overridden by --database)
    SNOWFLAKE_WAREHOUSE: Default warehouse (overridden by --warehouse)
    MIGRATION_SPREADSHEET_ID: Google Sheets spreadsheet ID
    GOOGLE_SHEETS_CREDENTIALS_FILE: Path to Google Sheets credentials file
        """
    )
    
    # Snowflake configuration
    parser.add_argument(
        "--database", 
        default=os.getenv("SNOWFLAKE_DATABASE"),
        help="Snowflake database name (default: from SNOWFLAKE_DATABASE env var)"
    )
    
    parser.add_argument(
        "--schema", 
        default="TEMP_ARGO_RAW",
        help="Snowflake schema name (default: TEMP_ARGO_RAW)"
    )
    
    parser.add_argument(
        "--role", 
        default="DBT_ROLE",
        help="Snowflake role to use (default: DBT_ROLE)"
    )
    
    parser.add_argument(
        "--warehouse", 
        default=os.getenv("SNOWFLAKE_WAREHOUSE"),
        help="Snowflake warehouse to use (default: from SNOWFLAKE_WAREHOUSE env var)"
    )
    
    parser.add_argument(
        "--output-dir", 
        default="sql/stg/",
        help="Directory to save SQL files (default: sql/stg/)"
    )
    
    # Google Sheets configuration
    parser.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        help="Path to Google Sheets credentials JSON file"
    )
    
    parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("MIGRATION_SPREADSHEET_ID"),
        help="Google Sheets spreadsheet ID"
    )
    
    # Options
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Run in read-only mode (don't update Check column in Google Sheets)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without creating files or updating sheets"
    )
    
    return parser.parse_args()

def main():
    """
    Main function to execute data extraction and SQL file generation.
    """
    # Show deprecation notice
    print("⚠️  LEGACY MODE: You're using the standalone script")
    print("📍 RECOMMENDED: Use 'python main.py generate-stg [options]' for the integrated version")
    print("=" * 80)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Validate required configuration
    if not args.database:
        print("❌ Error: Database not specified. Use --database or set SNOWFLAKE_DATABASE environment variable.")
        return 1
    
    if not args.credentials:
        print("❌ Error: Google Sheets credentials not specified. Use --credentials or set GOOGLE_SHEETS_CREDENTIALS_FILE environment variable.")
        return 1
        
    if not args.spreadsheet_id:
        print("❌ Error: Spreadsheet ID not specified. Use --spreadsheet-id or set MIGRATION_SPREADSHEET_ID environment variable.")
        return 1
    
    # Show configuration
    print("🚀 Starting staging files generation...")
    print("=" * 60)
    print(f"📊 Database: {args.database}")
    print(f"📂 Schema: {args.schema}")
    print(f"👤 Role: {args.role}")
    print(f"🏠 Warehouse: {args.warehouse}")
    print(f"📁 Output: {args.output_dir}")
    print(f"📄 Spreadsheet: {args.spreadsheet_id}")
    print(f"🔐 Credentials: {args.credentials}")
    
    if args.read_only:
        print("⚠️  Read-only mode: Will not update Check column")
    
    if args.dry_run:
        print("🧪 Dry-run mode: Will not create files or update sheets")
    
    print("=" * 60)
    
    # Get the data and Google Sheets client
    df, gsheets, spreadsheet_id = get_stg_files_data()
    
    if not df.empty:
        print("\n✅ Data extracted successfully.")
        
        if gsheets and spreadsheet_id and not args.read_only:
            print("✅ Google Sheets write permissions confirmed.")
        else:
            print("⚠️  Google Sheets updates disabled.")
            gsheets = None  # Disable writing if read-only mode
        
        if args.dry_run:
            print("\n🧪 Dry-run mode - showing what would be processed:")
            pending_rows = df[df['Check'].astype(str).str.lower() != 'true']
            completed_rows = df[df['Check'].astype(str).str.lower() == 'true']
            
            print(f"   ✅ Already completed: {len(completed_rows)} files")
            print(f"   🔄 Would process: {len(pending_rows)} files")
            
            if not pending_rows.empty:
                print("\n📋 Files that would be generated:")
                for _, row in pending_rows.head(10).iterrows():
                    print(f"   • {row['Model']} (from {row['Name']})")
                if len(pending_rows) > 10:
                    print(f"   ... and {len(pending_rows) - 10} more files")
            
            print("\n🧪 Dry-run completed. Use without --dry-run to actually generate files.")
            return 0
        
        # Generate SQL files automatically
        print("\n🔄 Starting automatic SQL file generation...")
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
        
        print("\n✅ Process completed successfully.")
        return 0
    else:
        print("\n❌ Could not obtain data.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
