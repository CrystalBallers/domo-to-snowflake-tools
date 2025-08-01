with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_INBOUND_SHIPMENT_ITEMS") }})

select
    "id" as id,
    "shipment_id" as shipment_id,
    "shipment_name" as shipment_name,
    "destination_fulfillment_center_id" as destination_fulfillment_center_id,
    "label_prep_type" as label_prep_type,
    "shipment_status" as shipment_status,
    "are_cases_required" as are_cases_required,
    "confirmed_need_by_date" as confirmed_need_by_date,
    "box_contents_source" as box_contents_source,
    "box_content_total_units" as box_content_total_units,
    "box_content_fee_per_unit" as box_content_fee_per_unit,
    "box_content_total_fee" as box_content_total_fee,
    "amazon_seller_id" as amazon_seller_id,
    "created_at" as created_at,
    "updated_at" as updated_at,
    "active" as active,
    "shipment_address_id" as shipment_address_id,
    "closed_at" as closed_at,
    "seller_sku" as seller_sku,
    "fulfillment_network_sku" as fulfillment_network_sku,
    "quantity_shipped" as quantity_shipped,
    "quantity_received" as quantity_received,
    "quantity_in_case" as quantity_in_case,
    "prep_details" as prep_details,
    "release_date" as release_date,
    "product_id" as product_id,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source