import pandas as pd
import os
import sys
from typing import Set, List
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from tools.utils.file_utils import ensure_output_dir, save_csv


def clean_and_deduplicate_ids(ids_string: str) -> str:
    """
    Clean and deduplicate a string of IDs separated by commas and line breaks.
    
    Args:
        ids_string: String with IDs separated by commas and/or line breaks
        
    Returns:
        String with unique, clean IDs separated by commas and line breaks
    """
    if pd.isna(ids_string) or not ids_string.strip():
        return ""
    
    # Split by commas and line breaks
    ids_list = []
    for part in str(ids_string).replace('\n', ',').split(','):
        cleaned_id = part.strip()
        if cleaned_id:  # Only add if not empty
            ids_list.append(cleaned_id)
    
    # Convert to set for deduplication and back to sorted list
    unique_ids = sorted(set(ids_list))
    
    # Join with commas and line breaks (original format)
    return ',\n'.join(unique_ids)


def process_dataflows_csv(input_file: str = "dataflows_encontrados.csv", 
                         output_file: str = "dataflows_encontrados_clean.csv"):
    """
    Process the dataflows CSV, deduplicate IDs and create a unique row for each Output Dataset ID.
    
    Args:
        input_file: Input CSV file
        output_file: Output CSV file with clean data
    """
    
    # Check if file exists - try organized path first
    input_path = ensure_output_dir(input_file, create_dirs=False)
    if not input_path.exists() and not os.path.exists(input_file):
        print(f"❌ Error: File '{input_file}' does not exist in current directory or {input_path}.")
        return
    
    # Use the file that exists
    actual_input_file = str(input_path) if input_path.exists() else input_file
    
    try:
        # Read the CSV
        print(f"📊 Reading file: {actual_input_file}")
        df = pd.read_csv(actual_input_file)
        
        # Check that expected columns exist
        required_columns = ['Dataflow ID', 'Source Dataset IDs', 'Output Dataset IDs']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Error: Missing the following columns in CSV: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            return
        
        print(f"✅ File read successfully. {len(df)} rows found.")
        
        # Show statistics before processing
        print("\n--- Statistics BEFORE processing ---")
        print(f"Total rows: {len(df)}")
        
        # List to store the new expanded rows
        expanded_rows = []
        
        print("\n🔧 Processing and expanding rows by Output Dataset ID...")
        
        for _, row in df.iterrows():
            dataflow_id = row['Dataflow ID']
            
            # Process Source Dataset IDs
            source_ids_clean = clean_and_deduplicate_ids(str(row['Source Dataset IDs']) if pd.notna(row['Source Dataset IDs']) else "")
            
            # Process Output Dataset IDs and separate them
            output_ids_clean = clean_and_deduplicate_ids(str(row['Output Dataset IDs']) if pd.notna(row['Output Dataset IDs']) else "")
            
            if output_ids_clean:
                # Separar los Output IDs individuales
                output_ids_list = [id_.strip() for id_ in output_ids_clean.replace('\n', ',').split(',') if id_.strip()]
                
                # Crear una fila para cada Output ID único
                for output_id in output_ids_list:
                    expanded_rows.append({
                        'Dataflow ID': dataflow_id,
                        'Source Dataset IDs': source_ids_clean,
                        'Output Dataset ID': output_id  # Singular ahora
                    })
            else:
                # Si no hay Output IDs, crear una fila con Output ID vacío
                expanded_rows.append({
                    'Dataflow ID': dataflow_id,
                    'Source Dataset IDs': source_ids_clean,
                    'Output Dataset ID': ""
                })
        
        # Crear el nuevo DataFrame expandido
        expanded_df = pd.DataFrame(expanded_rows)
        
        # Guardar el archivo procesado usando la nueva organización
        output_path = save_csv(expanded_df, output_file)
        print(f"\n✅ Archivo procesado guardado como: {output_path}")
        
        # Mostrar estadísticas después del procesamiento
        print("\n--- Estadísticas DESPUÉS del procesamiento ---")
        print(f"Filas totales: {len(expanded_df)}")
        
        # Contar IDs únicos
        unique_dataflows = expanded_df['Dataflow ID'].nunique()
        unique_output_ids = expanded_df['Output Dataset ID'].nunique()
        
        # Contar Source IDs únicos
        all_source_ids = set()
        for source_ids_str in expanded_df['Source Dataset IDs'].dropna():
            if source_ids_str.strip():
                source_ids = [id_.strip() for id_ in str(source_ids_str).replace('\n', ',').split(',') if id_.strip()]
                all_source_ids.update(source_ids)
        
        print(f"Dataflows únicos: {unique_dataflows}")
        print(f"Output Dataset IDs únicos: {unique_output_ids}")
        print(f"Source Dataset IDs únicos totales: {len(all_source_ids)}")
        
        # Mostrar algunos ejemplos de los primeros registros procesados
        print(f"\n--- Primeros 5 registros procesados ---")
        for i, (_, row) in enumerate(expanded_df.head(5).iterrows()):
            print(f"\nFila {i+1}:")
            print(f"  Dataflow ID: {row['Dataflow ID']}")
            print(f"  Output Dataset ID: {row['Output Dataset ID']}")
            print(f"  Source IDs: {str(row['Source Dataset IDs'])[:80]}{'...' if len(str(row['Source Dataset IDs'])) > 80 else ''}")
        
        # Mostrar estadísticas de expansión
        original_rows = len(df)
        expanded_rows_count = len(expanded_df)
        expansion_ratio = expanded_rows_count / original_rows if original_rows > 0 else 0
        print(f"\n📈 Expansión: {original_rows} → {expanded_rows_count} filas (ratio: {expansion_ratio:.2f}x)")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")


def main():
    """
    Main script function.
    """
    print("🧹 Starting dataflow IDs deduplication in CSV...")
    print("=" * 60)
    
    # Process the file
    process_dataflows_csv()
    
    print("\n" + "=" * 60)
    print("✅ Process completed.")


if __name__ == "__main__":
    main()