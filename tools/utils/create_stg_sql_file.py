import re

def create_stg_sql_file(columns: list[dict], source_schema_name: str, source_table_name: str, output_filename: str = "file.sql") -> str:
    """
    Crea un archivo SQL de staging a partir de una lista de columnas con sus tipos de datos.
    
    Args:
        columns: Lista de diccionarios con 'name' y 'data_type' keys
        source_schema_name: Nombre del schema fuente
        source_table_name: Nombre de la tabla fuente  
        output_filename: Nombre del archivo SQL a generar (por defecto "file.sql")
    
    Returns:
        str: El contenido SQL generado
    """
    
    def sanitize_column_name(col_name: str) -> str:
        """
        Convierte el nombre de columna a minúsculas, sustituye espacios por guiones bajos
        y elimina caracteres que no sean letras, números o guiones bajos.
        """
        alias = col_name.replace(" ", "_")
        alias = re.sub(r"[^0-9A-Za-z_]", "", alias)
        return alias.lower()

    def get_cast_expression(column_name: str, data_type: str) -> str:
        """
        Genera la expresión CAST apropiada basada en el tipo de dato de Snowflake.
        Aplica lógica inteligente para manejar conversiones Domo → Snowflake.
        
        Args:
            column_name: Nombre de la columna
            data_type: Tipo de dato de Snowflake
            
        Returns:
            str: Expresión SQL con CAST solo donde es necesario, keywords en minúsculas
        """
        # Convertir a mayúsculas para comparación, pero usar minúsculas en output
        data_type_upper = data_type.upper()
        data_type_lower = data_type.lower()
        
        # Tipos de fecha/tiempo: Domo STRING → Snowflake DATE/TIME (requieren CAST)
        if 'TIMESTAMP' in data_type_upper:
            return f'cast("{column_name}" as timestamp)'
        elif 'DATE' in data_type_upper:
            return f'cast("{column_name}" as date)'
        elif 'TIME' in data_type_upper:
            return f'cast("{column_name}" as time)'
        
        # Tipos numéricos: Domo STRING → Snowflake NUMERIC (requieren CAST)
        elif data_type_upper.startswith('NUMBER'):
            return f'cast("{column_name}" as {data_type_lower})'
        elif data_type_upper in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT']:
            return f'cast("{column_name}" as {data_type_lower})'
        elif data_type_upper in ['FLOAT', 'DOUBLE', 'REAL', 'DECIMAL', 'NUMERIC']:
            return f'cast("{column_name}" as {data_type_lower})'
        
        # Tipos booleanos: Domo 'true'/'false' → Snowflake BOOLEAN (requiere CAST)
        elif data_type_upper == 'BOOLEAN':
            return f'cast("{column_name}" as boolean)'
        
        # VARCHAR con longitud: Puede requerir CAST para longitud específica
        elif data_type_upper.startswith('VARCHAR'):
            return f'cast("{column_name}" as {data_type_lower})'
        
        # Tipos de texto simples: STRING, TEXT, etc. - CAST explícito para consistencia
        # Aunque sean compatibles, es mejor ser explícito para claridad y documentación
        else:
            return f'cast("{column_name}" as {data_type_lower})'

    def generate_sql(columns: list[dict]) -> str:
        """
        Dada una lista de columnas con tipos, genera el bloque SQL con CAST explícito en TODAS las columnas:
        
        with
            source as (select * from {{ source("SRC", "VW_TABLE_NAME") }})
        select
            cast("column_original" as varchar(255)) as column_original,
            cast("created_at" as timestamp) as created_at,
            cast("amount" as number(10,2)) as amount,
            ...
        from source
        """
        # Convertir tabla a mayúsculas para el source
        table_name_upper = source_table_name.upper()
        schema_name_upper = source_schema_name.upper()
        
        header = (
            "with\n"
            f"    source as (select * from {{{{ source(\"{schema_name_upper}\", \"{table_name_upper}\") }}}})\n\n"
            "select\n"
        )
        body_lines = [
            f'    {get_cast_expression(col["name"], col["data_type"])} as {sanitize_column_name(col["name"])}'
            for col in columns
        ]
        body = ",\n".join(body_lines)
        footer = "\n\nfrom source"
        return header + body + footer

    # Generar el SQL
    sql_content = generate_sql(columns)
    
    # Escribir el archivo
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(sql_content)
    
    print(f"Archivo '{output_filename}' generado exitosamente.")
    
    return sql_content


# Ejemplo de uso (solo para testing)
if __name__ == "__main__":
    # Lista de ejemplo para testing - estructura nueva con nombres y tipos
    example_columns = [
        {"name": "active", "data_type": "BOOLEAN"},
        {"name": "amazon_inbound_shipment_plan_id", "data_type": "VARCHAR(100)"},
        {"name": "amazon_reference_id", "data_type": "VARCHAR(50)"},
        {"name": "amazon_seller_id", "data_type": "VARCHAR(50)"},
        {"name": "are_cases_required", "data_type": "BOOLEAN"},
        {"name": "box_content_fee_per_unit", "data_type": "NUMBER(10,2)"},
        {"name": "box_content_total_fee", "data_type": "NUMBER(10,2)"},
        {"name": "box_content_total_units", "data_type": "INTEGER"},
        {"name": "box_contents_source", "data_type": "VARCHAR(100)"},
        {"name": "box_items_limit_allowed", "data_type": "INTEGER"},
        {"name": "carrier_description", "data_type": "VARCHAR(255)"},
        {"name": "carrier_id", "data_type": "VARCHAR(50)"},
        {"name": "closed_at", "data_type": "TIMESTAMP"},
        {"name": "confirmed_need_by_date", "data_type": "DATE"},
        {"name": "country_id", "data_type": "VARCHAR(10)"},
        {"name": "created_at", "data_type": "TIMESTAMP"},
        {"name": "deleted_at", "data_type": "TIMESTAMP"},
        {"name": "deleted_by", "data_type": "VARCHAR(100)"},
        {"name": "destination_fulfillment_center_id", "data_type": "VARCHAR(50)"},
        {"name": "expiration_dates_required", "data_type": "BOOLEAN"},
        {"name": "fc_prep_required", "data_type": "BOOLEAN"},
        {"name": "fnsku_or_upc", "data_type": "VARCHAR(50)"},
        {"name": "from_add_line1", "data_type": "VARCHAR(255)"},
        {"name": "from_add_line2", "data_type": "VARCHAR(255)"},
        {"name": "from_city", "data_type": "VARCHAR(100)"},
        {"name": "from_postal", "data_type": "VARCHAR(20)"},
        {"name": "from_state", "data_type": "VARCHAR(50)"},
        {"name": "gate_packing", "data_type": "VARCHAR(100)"},
        {"name": "id", "data_type": "INTEGER"},
        {"name": "is_ns_transfer_order", "data_type": "BOOLEAN"},
        {"name": "label_prep_type", "data_type": "VARCHAR(100)"},
        {"name": "labeling_required", "data_type": "BOOLEAN"},
        {"name": "max_number_items_per_box", "data_type": "INTEGER"},
        {"name": "ns_warehouse_name_id", "data_type": "VARCHAR(50)"},
        {"name": "number_of_pallets", "data_type": "INTEGER"},
        {"name": "overweight", "data_type": "BOOLEAN"},
        {"name": "overweight_max", "data_type": "NUMBER(10,2)"},
        {"name": "settlement_amount", "data_type": "NUMBER(15,2)"},
        {"name": "settlement_id", "data_type": "VARCHAR(100)"},
        {"name": "settlement_report_id", "data_type": "VARCHAR(100)"},
        {"name": "settlement_transaction_id", "data_type": "VARCHAR(100)"},
        {"name": "shipment_address_id", "data_type": "INTEGER"},
        {"name": "shipment_id", "data_type": "INTEGER"},
        {"name": "shipment_name", "data_type": "VARCHAR(255)"},
        {"name": "shipment_status", "data_type": "VARCHAR(50)"},
        {"name": "shipping_labels_1000", "data_type": "INTEGER"},
        {"name": "shipping_labels_required", "data_type": "BOOLEAN"},
        {"name": "tracking_no", "data_type": "VARCHAR(100)"},
        {"name": "transparent_by", "data_type": "VARCHAR(100)"},
        {"name": "transparent_on", "data_type": "TIMESTAMP"},
        {"name": "transport_estimated_cost", "data_type": "NUMBER(10,2)"},
        {"name": "transport_shipment_type", "data_type": "VARCHAR(50)"},
        {"name": "transport_status", "data_type": "VARCHAR(50)"},
        {"name": "updated_at", "data_type": "TIMESTAMP"},
        {"name": "updated_source", "data_type": "VARCHAR(100)"},
        {"name": "warehouse_id", "data_type": "INTEGER"},
        {"name": "weight_of_all_pallets", "data_type": "NUMBER(10,2)"},
        {"name": "whs_status_at", "data_type": "TIMESTAMP"},
        {"name": "whs_status_by", "data_type": "VARCHAR(100)"},
        {"name": "whs_status_id", "data_type": "INTEGER"},
        {"name": "whs_statuses_json", "data_type": "VARCHAR(16777216)"},
        {"name": "warehouse_shipped_date", "data_type": "DATE"},
        {"name": "_BATCH_ID_", "data_type": "VARCHAR(100)"},
        {"name": "_BATCH_LAST_RUN_", "data_type": "TIMESTAMP"}
    ]
    
    # Crear el archivo SQL usando las columnas de ejemplo
    create_stg_sql_file(
        columns=example_columns,
        source_schema_name="src",
        source_table_name="vw_amazon_shipments",
        output_filename="stg_amazon_shipments.sql"
    )