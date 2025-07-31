import pandas as pd
import os
from typing import Set, List


def clean_and_deduplicate_ids(ids_string: str) -> str:
    """
    Limpia y deduplica una cadena de IDs separados por comas y saltos de línea.
    
    Args:
        ids_string: Cadena con IDs separados por comas y/o saltos de línea
        
    Returns:
        Cadena con IDs únicos, limpios y separados por comas y saltos de línea
    """
    if pd.isna(ids_string) or not ids_string.strip():
        return ""
    
    # Dividir por comas y saltos de línea
    ids_list = []
    for part in str(ids_string).replace('\n', ',').split(','):
        cleaned_id = part.strip()
        if cleaned_id:  # Solo agregar si no está vacío
            ids_list.append(cleaned_id)
    
    # Convertir a set para deduplicar y luego de vuelta a lista ordenada
    unique_ids = sorted(set(ids_list))
    
    # Unir con comas y saltos de línea (formato original)
    return ',\n'.join(unique_ids)


def process_dataflows_csv(input_file: str = "dataflows_encontrados.csv", 
                         output_file: str = "dataflows_encontrados_clean.csv"):
    """
    Procesa el CSV de dataflows, deduplica los IDs y crea una fila única para cada Output Dataset ID.
    
    Args:
        input_file: Archivo CSV de entrada
        output_file: Archivo CSV de salida con datos limpios
    """
    
    # Verificar que el archivo existe
    if not os.path.exists(input_file):
        print(f"❌ Error: El archivo '{input_file}' no existe.")
        return
    
    try:
        # Leer el CSV
        print(f"📊 Leyendo archivo: {input_file}")
        df = pd.read_csv(input_file)
        
        # Verificar que las columnas esperadas existen
        required_columns = ['Dataflow ID', 'Source Dataset IDs', 'Output Dataset IDs']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Error: Faltan las siguientes columnas en el CSV: {missing_columns}")
            print(f"Columnas disponibles: {list(df.columns)}")
            return
        
        print(f"✅ Archivo leído correctamente. {len(df)} filas encontradas.")
        
        # Mostrar estadísticas antes del procesamiento
        print("\n--- Estadísticas ANTES del procesamiento ---")
        print(f"Filas totales: {len(df)}")
        
        # Lista para almacenar las nuevas filas expandidas
        expanded_rows = []
        
        print("\n🔧 Procesando y expandiendo filas por Output Dataset ID...")
        
        for _, row in df.iterrows():
            dataflow_id = row['Dataflow ID']
            
            # Procesar Source Dataset IDs
            source_ids_clean = clean_and_deduplicate_ids(str(row['Source Dataset IDs']) if pd.notna(row['Source Dataset IDs']) else "")
            
            # Procesar Output Dataset IDs y separarlos
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
        
        # Guardar el archivo procesado
        expanded_df.to_csv(output_file, index=False)
        print(f"\n✅ Archivo procesado guardado como: {output_file}")
        
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
        print(f"❌ Error al procesar el archivo: {e}")


def main():
    """
    Función principal del script.
    """
    print("🧹 Iniciando deduplicación de IDs en dataflows CSV...")
    print("=" * 60)
    
    # Procesar el archivo
    process_dataflows_csv()
    
    print("\n" + "=" * 60)
    print("✅ Proceso completado.")


if __name__ == "__main__":
    main()