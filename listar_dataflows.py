import logging
from tools.utils.domo import DomoHandler
from dotenv import load_dotenv

# Carga las variables de entorno desde un archivo .env si lo tienes
load_dotenv()

# Configura el logging para ver los mensajes de la herramienta
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Lista de Datasets Iniciales ---
# Reemplaza estos IDs con los datasets de los que quieres encontrar los dataflows
# Ejemplo: ["f2f7c2f8-9377-4158-9c6a-5b06a4a1a3b1"]
dataset_ids_iniciales = [
    "7d84cecd-9237-4932-bd0f-9240c72a1d14",
    "190b6a6c-934c-49a7-a939-c5af8e7635e0"
]

def main():
    """
    Función principal para encontrar y mostrar los dataflows relacionados.
    """
    if "reemplaza-con-tu-dataset-id" in dataset_ids_iniciales[0]:
        print("⚠️  Por favor, edita el archivo 'listar_dataflows.py' y reemplaza los IDs de dataset de ejemplo.")
        return

    print("Iniciando la búsqueda de dataflows...")

    # 1. Inicializar el handler de Domo
    domo_handler = DomoHandler()

    # 2. Configurar la autenticación (usa variables de entorno)
    if not domo_handler.setup_auth():
        print("❌ Error: No se pudo autenticar con Domo. Asegúrate de que tus variables de entorno (DOMO_INSTANCE, etc.) están configuradas.")
        return

    # 3. Llamar a la función para obtener todos los dataflows
    print(f"Buscando dataflows relacionados con {len(dataset_ids_iniciales)} datasets iniciales...")
    dataflows_df = domo_handler.get_all_dataflows(dataset_id_list=dataset_ids_iniciales)

    # 4. Mostrar los resultados
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