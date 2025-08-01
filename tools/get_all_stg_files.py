import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tools.utils.gsheets import GoogleSheets
from tools.utils.create_stg_sql_file import create_stg_sql_file
from tools.utils.snowflake import SnowflakeHandler

# Load environment variables
load_dotenv()

def get_stg_files_data():
    """
    Extracts data from the 'Stg Files' tab of the Google Spreadsheet.
    
    Returns:
        pd.DataFrame: Data from the 'Stg Files' tab
    """
    try:
        # Initialize Google Sheets client
        gsheets = GoogleSheets()
        
        # Get spreadsheet ID from environment variables
        spreadsheet_id = os.getenv("MIGRATION_SPREADSHEET_ID")
        if not spreadsheet_id:
            print("❌ Error: Environment variable MIGRATION_SPREADSHEET_ID is not configured.")
            return pd.DataFrame()
        
        # Read data from 'Stg Files' tab
        print("📊 Reading data from 'Stg Files' tab...")
        df = gsheets.read_to_dataframe(spreadsheet_id, "Stg Files!A:Z", header=True)
        
        if df.is_empty():
            print("❌ No data found in 'Stg Files' tab.")
            return pd.DataFrame()
        
        # Convert from polars to pandas
        pandas_df = df.to_pandas()
        
        print(f"✅ Found {len(pandas_df)} datasets to process.")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Error reading data from spreadsheet: {e}")
        return pd.DataFrame()


def generate_stg_files_from_dataframe(df: pd.DataFrame, database: str = None, schema: str = "raw_domo", output_dir: str = "sql/stg/", role: str = "DBT_ROLE"):
    """
    Iterates through each row of the DataFrame and generates staging SQL files.
    Gets real column names from Snowflake for each table.
    
    Args:
        df: DataFrame with data from the 'Stg Files' tab
        database: Snowflake database name (if None, uses environment variable)
        schema: Snowflake schema name (default: "raw_domo")
        output_dir: Directory where to save the SQL files
        role: Snowflake role to use (default: "DBT_ROLE")
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
    
    # Iterate through each row of the DataFrame
    for index, row in df.iterrows():
        dataset_id = row['Dataset ID']
        check_status = row['Check']
        name = row['Name']
        model_filename = row['Model']
        
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
            columns = snowflake_handler.get_table_columns(database, schema, name, role)
            
            if not columns:
                print(f"   ❌ Failed to get columns from Snowflake table: {database}.{schema}.{name}")
                print(f"   💡 Table may not exist or you may not have permissions with role: {role}")
                error_message = f"ERROR: Could not retrieve columns for table {database}.{schema}.{name} using role: {role}"
            else:
                print(f"   ✅ Found {len(columns)} columns from Snowflake")
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
    print(f"   ⚠️  Files skipped: {skipped_count}")
    print(f"   📁 Output directory: {output_dir}")
    print(f"   🗄️  Database used: {database}")
    print(f"   📂 Schema used: {schema}")
    print(f"   👤 Role used: {role}")
    print("=" * 80)


def main():
    """
    Main function to execute data extraction and SQL file generation.
    """
    print("🚀 Starting data extraction from 'Stg Files' tab...")
    print("=" * 60)
    
    # Get the data
    df = get_stg_files_data()
    
    if not df.empty:
        print("\n✅ Data extracted successfully.")
        
        # Generate SQL files automatically
        print("\n🔄 Starting automatic SQL file generation...")
        generate_stg_files_from_dataframe(df)
        
        print("\n✅ Process completed successfully.")
    else:
        print("\n❌ Could not obtain data.")
    
    print("=" * 60)


if __name__ == "__main__":
    main()
