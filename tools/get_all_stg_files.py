#!/usr/bin/env python3
"""
Generate staging SQL files from Google Sheets with Snowflake schema validation.

⚠️  DEPRECATION NOTICE: This standalone script is now integrated as a subcommand in main.py
📍 RECOMMENDED: Use `python main.py generate-stg [options]` instead

This script reads from the 'Stg Files' tab in Google Sheets and generates SQL files
with automatic CAST statements based on real Domo dataset schemas (source of truth).

Legacy Usage (still supported):
    python tools/get_all_stg_files.py [options]

Recommended Usage:
    python main.py generate-stg [options]
    
Features:
    - Source-first approach: Uses Domo dataset schemas as source of truth
    - Intelligent type mapping: Maps Domo types to Snowflake-compatible types  
    - Explicit CAST: All columns have explicit CAST statements for clarity
    - Smart skip: Only processes rows where Check != "True"  
    - Progress tracking: Updates Check column when files are created successfully
    - Optional validation: Compares Domo schema against existing Snowflake tables
"""

import os
import sys
import re
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
from tools.utils.domo import DomoHandler

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
        
        if df.empty:
            print("❌ No data found in 'Stg Files' tab.")
            return pd.DataFrame(), None, None
        
        # DataFrame is already pandas
        pandas_df = df
        
        print(f"✅ Found {len(pandas_df)} datasets to process.")
        
        return pandas_df, gsheets, spreadsheet_id
        
    except Exception as e:
        print(f"❌ Error reading data from spreadsheet: {e}")
        return pd.DataFrame(), None, None


def generate_stg_files_from_dataframe(df: pd.DataFrame, database: str = None, schema: str = "TEMP_ARGO_RAW", output_dir: str = "sql/stg/", role: str = "DBT_ROLE", warehouse: str = None, gsheets=None, spreadsheet_id: str = None):
    """
    Iterates through each row of the DataFrame and generates staging SQL files.
    Gets real column names and types from Domo datasets (source of truth).
    Maps Domo types to Snowflake-compatible types with intelligent CAST.
    Writes "True" to the Check column when files are created successfully.
    
    Args:
        df: DataFrame with data from the 'Stg Files' tab
        database: Snowflake database name (for validation only, if None uses environment variable)
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
    
    # Initialize Domo connection to get real column names and types from source
    print("🔗 Connecting to Domo to get dataset schemas...")
    domo_handler = DomoHandler()
    
    if not domo_handler.setup_auth():
        print("❌ Failed to connect to Domo. Cannot generate files without source schema.")
        return
    else:
        print("✅ Connected to Domo successfully.")
    
    # Initialize Snowflake connection for validation (optional)
    print("🔗 Connecting to Snowflake for validation...")
    snowflake_handler = SnowflakeHandler()
    
    if not snowflake_handler.setup_connection():
        print("⚠️  Failed to connect to Snowflake. Will generate files without validation.")
        snowflake_handler = None
    else:
        print("✅ Connected to Snowflake successfully.")
    
    print(f"\n🔄 Processing {len(df)} datasets (validate → create immediately)...")
    print("=" * 80)
    
    generated_count = 0
    skipped_count = 0
    already_completed_count = 0
    
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
        
        print(f"\n🔍 Row {index + 1}: {model_filename}")
        print(f"   Dataset ID: {dataset_id}")
        print(f"   Name: {name}")
        
        # Validate: Get column names from Snowflake and types from Domo
        columns = None
        error_message = None
        
        def normalize_column_name(col_name: str) -> str:
            """Normalize column name for matching by removing special chars and lowercasing."""
            import re
            return re.sub(r'[^a-z0-9]', '', col_name.lower())
        
        def map_domo_type_to_snowflake(domo_type: str) -> str:
            """Mapea tipos de Domo a tipos compatibles de Snowflake."""
            domo_type_upper = domo_type.upper()
            
            # Mapeo de tipos Domo → Snowflake compatible
            if domo_type_upper in ['DATETIME', 'DATE', 'TIMESTAMP']:
                return 'timestamp'
            elif domo_type_upper in ['DOUBLE', 'DECIMAL']:
                return 'float'  # Usar float para números con decimales
            elif domo_type_upper == 'LONG':
                return 'number'  # Mantener number solo para enteros
            elif domo_type_upper == 'STRING':
                return 'varchar(16777216)'  # Snowflake max VARCHAR
            elif domo_type_upper == 'TEXT':
                return 'text'
            else:
                # Para tipos desconocidos, usar VARCHAR como fallback seguro
                return 'varchar(16777216)'
        
        # Step 1: Get column names from Snowflake (normalized, clean names)
        print(f"   🔍 Getting column names from Snowflake table: {database}.{schema}.{name}")
        sf_columns = None
        if snowflake_handler:
            sf_columns = snowflake_handler.get_table_columns(database, schema, name, role, warehouse)
            if sf_columns:
                print(f"   ✅ Found {len(sf_columns)} columns from Snowflake")
            else:
                print(f"   ⚠️  Could not get Snowflake columns (table may not exist yet)")
        
        # Step 2: Get data sample from Domo to infer types
        print(f"   🔍 Getting data types from Domo dataset: {dataset_id}")
        domo_sample = None
        domo_types_map = {}
        
        try:
            # Extract small sample to infer types
            domo_sample = domo_handler.extract_data(dataset_id, "SELECT * FROM table LIMIT 5", chunk_size=999999999)
            
            if domo_sample is not None and not domo_sample.empty:
                print(f"   ✅ Extracted sample from Domo: {len(domo_sample.columns)} columns")
                
                # Infer types from real data
                for col_name in domo_sample.columns:
                    col_dtype = domo_sample[col_name].dtype
                    
                    # Map pandas dtypes to Domo-like types
                    if pd.api.types.is_integer_dtype(col_dtype):
                        domo_type = 'LONG'
                    elif pd.api.types.is_float_dtype(col_dtype):
                        domo_type = 'DOUBLE'
                    elif pd.api.types.is_datetime64_any_dtype(col_dtype):
                        domo_type = 'DATETIME'
                    else:
                        # Check for date-like strings in first few values
                        sample_values = domo_sample[col_name].dropna().astype(str).head(3)
                        date_like = False
                        numeric_like = False
                        
                        for val in sample_values:
                            # Check if looks like date - BE MORE STRICT
                            date_patterns = [
                                r'^\d{4}-\d{2}-\d{2}',           # 2024-01-15
                                r'^\d{2}/\d{2}/\d{4}',           # 01/15/2024
                                r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}', # 2024-01-15 14:30
                                r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}', # 2024-01-15T14:30
                            ]
                            
                            # Only consider it a date if it matches specific patterns
                            val_str = str(val).strip()
                            for pattern in date_patterns:
                                if re.match(pattern, val_str):
                                    # Double-check it's actually parseable as date
                                    try:
                                        pd.to_datetime(val_str)
                                        date_like = True
                                        break
                                    except:
                                        pass
                            
                            if date_like:
                                break
                            
                            # Check if looks like number
                            try:
                                float(val)
                                numeric_like = True
                            except:
                                pass
                        
                        if date_like:
                            domo_type = 'DATETIME'
                        elif numeric_like:
                            # Be more intelligent about detecting numeric columns
                            col_lower = col_name.lower()
                            numeric_keywords = [
                                'id', 'count', 'amount', 'price', 'quantity', 'number', 'total',
                                'threshold', 'value', 'rate', 'percent', 'score', 'metric',
                                'weight', 'size', 'length', 'width', 'height', 'volume',
                                'cost', 'revenue', 'profit', 'margin', 'ratio', 'factor'
                            ]
                            
                            # Check if column name contains numeric keywords
                            is_numeric_column = any(keyword in col_lower for keyword in numeric_keywords)
                            
                            if is_numeric_column:
                                domo_type = 'DOUBLE'
                            else:
                                domo_type = 'STRING'  # Even if numeric values, treat as string if name doesn't suggest numeric use
                        else:
                            domo_type = 'STRING'
                    
                    # Store the mapping with normalized name for matching
                    normalized_name = normalize_column_name(col_name)
                    domo_types_map[normalized_name] = {
                        'original_name': col_name,
                        'type': domo_type,
                        'snowflake_type': map_domo_type_to_snowflake(domo_type)
                    }
                
                print(f"   ✅ Inferred types for {len(domo_types_map)} Domo columns")
                # Show sample of inferred types
                sample_types = list(domo_types_map.items())[:3]
                for norm_name, info in sample_types:
                    print(f"      • {info['original_name']} ({info['type']} → {info['snowflake_type']})")
                if len(domo_types_map) > 3:
                    print(f"      ... and {len(domo_types_map) - 3} more columns")
            else:
                print(f"   ⚠️  Could not extract sample from Domo dataset")
        except Exception as e:
            print(f"   ⚠️  Error extracting Domo sample: {e}")
        
        # Step 3: Validation - Match Snowflake columns with Domo types
        if sf_columns and domo_types_map:
            print(f"   🔗 Validating Snowflake columns with Domo types...")
            
            matched_columns = []
            unmatched_sf_columns = []
            unmatched_domo_columns = list(domo_types_map.keys())
            
            for sf_col in sf_columns:
                sf_name = sf_col['name']
                sf_normalized = normalize_column_name(sf_name)
                
                if sf_normalized in domo_types_map:
                    # Match found!
                    domo_info = domo_types_map[sf_normalized]
                    matched_columns.append({
                        'name': sf_name,  # Use Snowflake name (clean)
                        'data_type': domo_info['snowflake_type'],  # Use Domo type mapped to Snowflake
                        'domo_type': domo_info['type'],
                        'domo_name': domo_info['original_name']
                    })
                    # Safe remove: only remove if it exists in the list
                    if sf_normalized in unmatched_domo_columns:
                        unmatched_domo_columns.remove(sf_normalized)
                else:
                    # Snowflake column not found in Domo
                    unmatched_sf_columns.append(sf_col)
            
            print(f"   ✅ Matched {len(matched_columns)} columns")
            print(f"   ⚠️  Unmatched Snowflake: {len(unmatched_sf_columns)}")
            print(f"   ⚠️  Unmatched Domo: {len(unmatched_domo_columns)}")
            
            columns = matched_columns
            
            # Note: Unmatched Snowflake columns will be added as comments only (not as regular columns)
            
            # Store unmatched Domo columns for comments
            unmatched_domo_info = []
            for norm_name in unmatched_domo_columns:
                domo_info = domo_types_map[norm_name]
                unmatched_domo_info.append({
                    'name': domo_info['original_name'],
                    'data_type': domo_info['snowflake_type'],
                    'domo_type': domo_info['type'],
                    'domo_name': domo_info['original_name']
                })
            
            # Add both types of unmatched columns as comments
            if unmatched_domo_info:
                print(f"   📝 Will add {len(unmatched_domo_info)} unmatched Domo columns as comments")
                # Store for later use in SQL generation
                columns.extend([{**col, 'commented': True, 'comment_type': 'domo_only'} for col in unmatched_domo_info])
            
            if unmatched_sf_columns:
                print(f"   📝 Will add {len(unmatched_sf_columns)} Snowflake-only columns as comments")
                # Store for later use in SQL generation
                for sf_col in unmatched_sf_columns:
                    columns.append({
                        'name': sf_col['name'],
                        'data_type': sf_col['data_type'].lower(),
                        'domo_type': 'UNKNOWN',
                        'domo_name': f"MISSING_IN_DOMO_{sf_col['name']}",
                        'commented': True,
                        'comment_type': 'snowflake_only'
                    })
                
        elif sf_columns and not domo_types_map:
            print(f"   ⚠️  Using Snowflake columns with default types (no Domo sample)")
            columns = []
            for sf_col in sf_columns:
                columns.append({
                    'name': sf_col['name'],
                    'data_type': sf_col['data_type'].lower(),  # Use Snowflake type
                    'domo_type': 'UNKNOWN',
                    'domo_name': sf_col['name']
                })
        elif domo_types_map and not sf_columns:
            print(f"   ❌ Snowflake table {database}.{schema}.{name} not found or accessible")
            print(f"   💡 Cannot create STG file without target Snowflake table")
            error_message = f"ERROR: Snowflake table {database}.{schema}.{name} does not exist or is not accessible with role {role}"
            columns = None
        else:
            print(f"   ❌ Could not get columns from either Snowflake or Domo")
            error_message = f"ERROR: Could not retrieve columns from Snowflake table {database}.{schema}.{name} or Domo dataset {dataset_id}"
        
        # Validation → Immediate creation
        if error_message:
            print(f"   ❌ SKIPPING: {error_message}")
            skipped_count += 1
        else:
            print(f"   ✅ Validation passed: {len(columns) if columns else 0} columns")
            
            # Create file immediately
            try:
                output_path = os.path.join(output_dir, model_filename)
                
                # Call the create_stg_sql_file function
                sql_content = create_stg_sql_file(
                    columns=columns,
                    source_schema_name=schema,
                    source_table_name=name,
                    output_filename=output_path
                )
                
                print(f"   🎯 Created: {output_path}")
                generated_count += 1
                
                # Update Google Sheets
                if gsheets and spreadsheet_id:
                    try:
                        check_cell = f"Stg Files!B{index + 2}"
                        gsheets.write_range(spreadsheet_id, check_cell, [["True"]])
                        print(f"   📝 Updated Check column to 'True'")
                    except Exception as write_error:
                        print(f"   ⚠️  Warning: Could not update Check column: {write_error}")
                
            except Exception as e:
                print(f"   ❌ Error creating file: {e}")
                skipped_count += 1
    
    # Cleanup connections
    if domo_handler:
        print("🔌 Domo connection closed.")
    if snowflake_handler:
        snowflake_handler.cleanup()
        print("🔌 Snowflake connection closed.")
    
    # Final summary
    print("\n" + "=" * 80)
    print(f"📊 SUMMARY:")
    print(f"   🎯 Files created: {generated_count}")
    print(f"   ✅ Already completed: {already_completed_count}")
    print(f"   ⚠️  Files skipped: {skipped_count}")
    print(f"   📁 Output: {output_dir}")
    print(f"   🗄️  Database: {database}")
    print(f"   📂 Schema: {schema}")
    print(f"   👤 Role: {role}")
    if gsheets and spreadsheet_id:
        print(f"   📝 Google Sheets updates: Enabled")
    else:
        print(f"   📝 Google Sheets updates: Disabled")
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
