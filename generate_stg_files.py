#!/usr/bin/env python3
"""
Simple script to generate staging SQL files from Google Spreadsheet data.
"""

from tools.get_all_stg_files import get_stg_files_data, generate_stg_files_from_dataframe

if __name__ == "__main__":
    # ========================================
    # CONFIGURACIÓN - MODIFICA ESTAS VARIABLES
    # ========================================
    
    # Snowflake configuration
    DATABASE = "DW_REPORTS"  # Cambia por tu database
    SCHEMA = "TEMP_ARGO_RAW"      # Cambia por tu schema
    ROLE = "DBT_ROLE"        # Cambia por tu role
    OUTPUT_DIR = "sql/stg/"  # Directorio donde guardar los archivos
    
    # ========================================
    # EJECUCIÓN - NO MODIFICAR DESDE AQUÍ
    # ========================================
    
    print("🚀 Starting staging files generation...")
    print(f"📊 Database: {DATABASE}")
    print(f"📂 Schema: {SCHEMA}")
    print(f"👤 Role: {ROLE}")
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
            role=ROLE
        )
    else:
        print("❌ No data found to process.") 