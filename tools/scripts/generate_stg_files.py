#!/usr/bin/env python3
"""
Simple script to generate staging SQL files from Google Spreadsheet data.
"""

from tools.get_all_stg_files import get_stg_files_data, generate_stg_files_from_dataframe

if __name__ == "__main__":
    # ========================================
    # CONFIGURATION - MODIFY THESE VARIABLES
    # ========================================
    
    # Snowflake configuration
    DATABASE = "DW_REPORTS"  # Change to your database
    SCHEMA = "TEMP_ARGO_RAW"      # Change to your schema
    ROLE = "DBT_ROLE"        # Change to your role
    WAREHOUSE = "DBT_WH"      # Change to your warehouse
    OUTPUT_DIR = "sql/stg/"  # Directory to save files
    
    # ========================================
    # EJECUCIÓN - NO MODIFICAR DESDE AQUÍ
    # ========================================
    
    print("🚀 Starting staging files generation...")
    print(f"📊 Database: {DATABASE}")
    print(f"📂 Schema: {SCHEMA}")
    print(f"👤 Role: {ROLE}")
    print(f"🏠 Warehouse: {WAREHOUSE}")
    print(f"📁 Output: {OUTPUT_DIR}")
    print("=" * 50)
    
    # Get data from Google Spreadsheet
    df = get_stg_files_data()
    
    if not df.empty:
        # Generate SQL files
        generate_stg_files_from_dataframe(
            df=df,
            database=DATABASE,
            schema=SCHEMA,
            output_dir=OUTPUT_DIR,
            role=ROLE,
            warehouse=WAREHOUSE
        )
    else:
        print("❌ No data found to process.") 