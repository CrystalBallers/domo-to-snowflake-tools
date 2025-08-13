import logging
import os
from tools.utils.domo import DomoHandler
from tools.utils.gsheets import GoogleSheets
from dotenv import load_dotenv

# Load environment variables from .env file if you have one
load_dotenv()

# Configure logging to see tool messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_dataset_ids_from_gsheets():
    """
    Get all Dataset IDs from the 'All Datasets' tab of the Google Spreadsheet.
    
    Returns:
        List[str]: List of Dataset IDs extracted from the 'Dataset ID' column
    """
    try:
        # Inicializar cliente de Google Sheets
        gsheets = GoogleSheets()
        
        # Obtener el ID del spreadsheet desde variables de entorno
        spreadsheet_id = os.getenv("MIGRATION_SPREADSHEET_ID")
        if not spreadsheet_id:
            print("❌ Error: La variable de entorno GOOGLE_SPREADSHEET_ID no está configurada.")
            return []
        
        # Leer datos de la pestaña 'All Datasets'
        print("📊 Leyendo datos de la pestaña 'All Datasets'...")
        df = gsheets.read_to_dataframe(spreadsheet_id, "All Datasets!A:Z", header=True)
        
        if df.is_empty():
            print("❌ No se encontraron datos en la pestaña 'All Datasets'.")
            return []
        
        # Verificar que existe la columna 'Dataset ID'
        if "Dataset ID" not in df.columns:
            print("❌ No se encontró la columna 'Dataset ID' en la pestaña 'All Datasets'.")
            print(f"Columnas disponibles: {df.columns}")
            return []
        
        # Extraer los Dataset IDs (filtrar valores vacíos/nulos)
        dataset_ids = df.select("Dataset ID").to_series().drop_nulls().to_list()
        dataset_ids = [str(id_).strip() for id_ in dataset_ids if str(id_).strip()]
        
        print(f"✅ Se encontraron {len(dataset_ids)} Dataset IDs en el spreadsheet.")
        return dataset_ids
        
    except Exception as e:
        print(f"❌ Error al leer datos del spreadsheet: {e}")
        return []

def main():
    """
    Función principal para encontrar y mostrar los dataflows relacionados.
    """
    print("Iniciando la búsqueda de dataflows...")

    # 1. Obtener los Dataset IDs desde Google Sheets
    dataset_ids_iniciales = get_dataset_ids_from_gsheets()
    
    if not dataset_ids_iniciales:
        print("❌ No se pudieron obtener Dataset IDs del spreadsheet. Verifica la configuración.")
        return

    # 2. Inicializar el handler de Domo
    domo_handler = DomoHandler()

    # 3. Configurar la autenticación (usa variables de entorno)
    if not domo_handler.setup_auth():
        print("❌ Error: No se pudo autenticar con Domo. Asegúrate de que tus variables de entorno (DOMO_INSTANCE, etc.) están configuradas.")
        return

    # 4. Llamar a la función para obtener todos los dataflows
    print(f"Buscando dataflows relacionados con {len(dataset_ids_iniciales)} datasets iniciales...")
    dataflows_df = domo_handler.get_all_dataflows(dataset_id_list=dataset_ids_iniciales)

    # 5. Mostrar los resultados
    if dataflows_df is not None and not dataflows_df.empty:
        print("\n\n\n\n\n\n--- ✅ Dataflows Encontrados ---")
        print(dataflows_df.to_string())

        # Opcional: Guardar los resultados en un archivo CSV
        output_csv = "dataflows_encontrados.csv"
        dataflows_df.to_csv(output_csv, index=False)
        print(f"\nResultados también guardados en: {output_csv}")
    else:
        print("\n🤷 No se encontraron dataflows para los datasets proporcionados.")

if __name__ == "__main__":
    main() 