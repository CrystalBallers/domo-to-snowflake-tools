import re

# VARIABLES GLOBALES (¡modifícalas aquí!)
NAME = "TEMP_ARGO_RAW"
DATABASE_NAME = "DW_REPORTS"
SCHEMA_NAME = "TEMP_ARGO_RAW"
TABLE_NAMES = [
    "AMAZON_FBA_INVENTORY_HEALTH",
    "AMAZON_FBA_LONGTERM_STORAGE_FEE_CHARGES",
    "AMAZON_FBA_REMOVAL_ORDER_DETAIL",
    "AMAZON_FBA_STORAGE_FEE_CHARGES",
    "AMAZON_FULFILLMENTS",
    "AMAZON_HIGH_RETURN_RATE_FEES",
    "AMAZON_INBOUND_PLACEMENT_FEES",
    "AMAZON_INBOUND_SHIPMENT_ITEMS",
    "AMAZON_INBOUND_SHIPMENTS",
    "AMAZON_ORDER_ITEMS_MFN",
    "AMAZON_SETTLEMENT_ITEMS",
    "AMAZON_SETTLEMENTS",
    "CALENDAR_DAYS_WEEKS_CONSTANTS",
    "COBALT_MAIN_BRAND_SUB_CAT_YM_MARKETIQ",
    "DEFAULT_CLASS_PARTNER_BRAND_BY_VENDORXLSX",
    "DUMMY_PORTFOLIOS",
    "EXCLUDE_FROM_NS_VID_OFFERSXLSX",
    "FEE_PREVIEW",
    "NG_COGS_HISTORICAL",
    "NG_PRICING_LEVELS",
    "NS_AMAZON_OFFERS",
    "NS_ITEM_INVENTORY_BY_LOCATIONS_NG",
    "NS_ITEM_VENDOR_MEMBERS",
    "NS_NGCOGS_2025_YTD",
    "OBSOLETE_ATLANTA_FIREFLY_DAILY_PRODUCTS_DIM",
    "PCN_OFFERS_LATEST",
    "POSTAL_CODES",
    "RAIDAR_AUDIT_TABLE",
    "RAIDAR_WORKING_DB_TABLE",
    "REPURPOSE_SETTLEMENTS_CACHE",
    "STATIC_ATLANTA_FIREFLY_DAILY_PARTNER_SKU_SALES_COGS",
    "USER_INPUT_REQUIRED"
]

OUTPUT_FILE = 'sources_auto.yml'

def sanitize(name):
    """
    Elimina todo carácter que no sea letra, número o espacio,
    reemplaza espacios por guiones bajos, pasa a minúsculas
    y añade el prefijo 'raw_'.
    """
    cleaned = re.sub(r'[^0-9a-zA-Z_ ]+', '', name)
    cleaned = cleaned.replace(' ', '_').upper()
    return f"{cleaned}"

def generate_sources(name, database_name, schema_name, table_names, output_file):
    """
    Genera un archivo 'sources.yml' con el esquema y las tablas dadas.
    """
    with open(output_file, 'w') as f:
        f.write("version: 2\n\n")
        f.write("sources:\n")
        f.write(f"  - name: {name}\n")
        f.write(f"    database: {database_name}\n")
        f.write(f"    schema: {schema_name}\n")
        f.write(f"    tables:\n")
        for name in table_names:
            identifier = sanitize(name)
            f.write(f"      - name: {name}\n")
            f.write(f"        identifier: {identifier}\n")

if __name__ == "__main__":
    generate_sources(NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAMES, OUTPUT_FILE)
    print(f"'{OUTPUT_FILE}' generado con esquema '{SCHEMA_NAME}' y tablas: {TABLE_NAMES}")