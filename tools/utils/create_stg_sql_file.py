import re

def create_stg_sql_file(columns: list[str], source_schema_name: str, source_table_name: str, output_filename: str = "file.sql") -> str:
    """
    Crea un archivo SQL de staging a partir de una lista de columnas.
    
    Args:
        columns: Lista de nombres de columnas
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

    def generate_sql(columns: list[str]) -> str:
        """
        Dada una lista de nombres de columna, genera el bloque SQL:
        
        with
            source as (select * from {{ source("source1", "source2") }})
        select
            "Columna Original" as columna_original,
            ...
        from source
        """
        header = (
            "with\n"
            f"    source as (select * from {{{{ source(\"{source_schema_name}\", \"{source_table_name}\") }}}})\n\n"
            "select\n"
        )
        body_lines = [
            f'    "{col}" as {sanitize_column_name(col)}'
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
    # Lista de ejemplo para testing
    example_columns = [
        "active",
        "amazon_inbound_shipment_plan_id",
        "amazon_reference_id",
        "amazon_seller_id",
        "are_cases_required",
        "box_content_fee_per_unit",
        "box_content_total_fee",
        "box_content_total_units",
        "box_contents_source",
        "box_items_limit_allowed",
        "carrier_description",
        "carrier_id",
        "closed_at",
        "confirmed_need_by_date",
        "country_id",
        "created_at",
        "deleted_at",
        "deleted_by",
        "destination_fulfillment_center_id",
        "expiration_dates_required",
        "fc_prep_required",
        "fnsku_or_upc",
        "from_add_line1",
        "from_add_line2",
        "from_city",
        "from_postal",
        "from_state",
        "gate_packing",
        "id",
        "is_ns_transfer_order",
        "label_prep_type",
        "labeling_required",
        "max_number_items_per_box",
        "ns_warehouse_name_id",
        "number_of_pallets",
        "overweight",
        "overweight_max",
        "settlement_amount",
        "settlement_id",
        "settlement_report_id",
        "settlement_transaction_id",
        "shipment_address_id",
        "shipment_id",
        "shipment_name",
        "shipment_status",
        "shipping_labels_1000",
        "shipping_labels_required",
        "tracking_no",
        "transparent_by",
        "transparent_on",
        "transport_estimated_cost",
        "transport_shipment_type",
        "transport_status",
        "updated_at",
        "updated_source",
        "warehouse_id",
        "weight_of_all_pallets",
        "whs_status_at",
        "whs_status_by",
        "whs_status_id",
        "whs_statuses_json",
        "warehouse_shipped_date",
        "_BATCH_ID_",
        "_BATCH_LAST_RUN_"
    ]
    
    # Crear el archivo SQL usando las columnas de ejemplo
    create_stg_sql_file(
        columns=example_columns,
        source_schema_name="raw_domo",
        source_table_name="amazon_shipments",
        output_filename="stg_amazon_shipments.sql"
    )