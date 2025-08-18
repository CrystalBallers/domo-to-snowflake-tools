import re
import os
import sys

# Add project root to path to import from tools
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..', '..')
sys.path.insert(0, project_root)

from tools.get_all_stg_files import get_stg_files_data

def sanitize(name):
    """
    Elimina todo carácter que no sea letra, número o espacio,
    reemplaza espacios por guiones bajos, pasa a minúsculas
    y añade el prefijo 'raw_'.
    """
    cleaned = re.sub(r'[^0-9a-zA-Z_ ]+', '', name)
    cleaned = cleaned.replace(' ', '_').upper()
    return f"{cleaned}"

def generate_sources_from_spreadsheet(database, schema, output_file='sources_auto.yml'):
    """
    Genera un archivo 'sources.yml' usando los datos del Google Sheets.
    Lee la columna 'Name' para obtener los nombres de las tablas.
    """
    print(f"📊 Reading Google Sheets data...")
    
    # Get data from Google Sheets (same as generate-stg command)
    df, gsheets, spreadsheet_id = get_stg_files_data()
    
    if df is None or df.empty:
        print("❌ No data found in Google Sheets")
        return False
    
    # Filter out rows where Check is True (already processed)
    df_filtered = df[df['Check'] != True].copy()
    
    if df_filtered.empty:
        print("✅ All tables already have Check = True, no sources to generate")
        return True
    
    print(f"📋 Found {len(df_filtered)} tables to include in sources.yml")
    
    # Extract table names from Name column
    table_names = df_filtered['Name'].dropna().tolist()
    
    if not table_names:
        print("❌ No table names found in 'Name' column")
        return False
    
    # Generate the sources file
    source_name = schema  # Use schema as the source name
    
    with open(output_file, 'w') as f:
        f.write("version: 2\n\n")
        f.write("sources:\n")
        f.write(f"  - name: {source_name}\n")
        f.write(f"    database: {database}\n")
        f.write(f"    schema: {schema}\n")
        f.write(f"    tables:\n")
        
        for table_name in table_names:
            identifier = sanitize(table_name)
            f.write(f"      - name: {table_name.upper()}\n")  # Force uppercase names
            f.write(f"        identifier: {identifier}\n")
    
    print(f"✅ Generated '{output_file}' with {len(table_names)} tables")
    print(f"   📊 Database: {database}")
    print(f"   📂 Schema: {schema}")
    print(f"   📋 Tables: {len(table_names)} from Google Sheets")
    
    return True

def generate_sources(name, database_name, schema_name, table_names, output_file):
    """
    Genera un archivo 'sources.yml' con el esquema y las tablas dadas.
    (Legacy function for backward compatibility)
    """
    with open(output_file, 'w') as f:
        f.write("version: 2\n\n")
        f.write("sources:\n")
        f.write(f"  - name: {name}\n")
        f.write(f"    database: {database_name}\n")
        f.write(f"    schema: {schema_name}\n")
        f.write(f"    tables:\n")
        for table_name in table_names:
            identifier = sanitize(table_name)
            f.write(f"      - name: {table_name.upper()}\n")  # Force uppercase names
            f.write(f"        identifier: {identifier}\n")

if __name__ == "__main__":
    # Default values for standalone execution
    DEFAULT_DATABASE = "DW_RAW"
    DEFAULT_SCHEMA = "SRC"
    DEFAULT_OUTPUT = "sources_auto.yml"
    
    import argparse
    parser = argparse.ArgumentParser(description='Generate dbt sources.yml from Google Sheets')
    parser.add_argument('--database', default=DEFAULT_DATABASE, help='Snowflake database name')
    parser.add_argument('--schema', default=DEFAULT_SCHEMA, help='Snowflake schema name')  
    parser.add_argument('--output', default=DEFAULT_OUTPUT, help='Output file name')
    
    args = parser.parse_args()
    
    success = generate_sources_from_spreadsheet(args.database, args.schema, args.output)
    if not success:
        sys.exit(1)