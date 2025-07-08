# Test Setup Instructions

## Para ejecutar el test de Google Sheets

### 1. Configurar credenciales

Necesitas configurar las credenciales de Google Sheets en tu archivo `.env`:

```bash
# Agregar al archivo .env
GOOGLE_SHEETS_CREDENTIALS_FILE=/path/to/your/service-account-key.json
```

### 2. Obtener credenciales de Service Account

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Crea o selecciona un proyecto
3. Habilita la Google Sheets API
4. Crea un Service Account
5. Descarga el archivo JSON de credenciales
6. Guárdalo en tu proyecto (asegúrate de que esté en .gitignore)

### 3. Compartir el spreadsheet

El spreadsheet debe estar compartido con el email del Service Account:

1. Abre el spreadsheet: https://docs.google.com/spreadsheets/d/1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA/edit
2. Haz clic en "Compartir"
3. Agrega el email del Service Account (se encuentra en el archivo JSON)
4. Dale permisos de "Viewer" o "Editor"

### 4. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 5. Ejecutar el test

```bash
python test_google_sheets.py
```

## Lo que hace el test

1. **Conecta** a Google Sheets usando las credenciales
2. **Lista** todos los tabs disponibles en el spreadsheet
3. **Lee** el contenido del tab "Inventory"
4. **Imprime** los datos en la consola
5. **Guarda** los datos en un archivo CSV local (`inventory_data.csv`)

## Salida esperada

El test mostrará:
- ✅ Conexión exitosa
- 📋 Propiedades del spreadsheet
- 📑 Lista de tabs disponibles
- 📊 Datos del tab "Inventory"
- 💾 Archivo CSV guardado localmente

## Troubleshooting

### Error: "Credentials file not found"
- Verifica que la ruta en `GOOGLE_SHEETS_CREDENTIALS_FILE` sea correcta
- Asegúrate de que el archivo JSON existe

### Error: "Permission denied"
- Verifica que el spreadsheet esté compartido con el Service Account
- Revisa que el Service Account tenga los permisos correctos

### Error: "API not enabled"
- Habilita la Google Sheets API en Google Cloud Console

### Error: "Worksheet not found"
- Verifica que el tab "Inventory" exista en el spreadsheet
- Revisa que el nombre del tab sea exacto (case-sensitive) 