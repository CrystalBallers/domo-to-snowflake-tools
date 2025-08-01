with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_FBA_REMOVAL_ORDER_DETAIL") }})

select
    "amazon_seller_id" as amazon_seller_id,
    "cancelled_quantity" as cancelled_quantity,
    "created_at" as created_at,
    "currency" as currency,
    "disposed_quantity" as disposed_quantity,
    "disposition" as disposition,
    "fnsku" as fnsku,
    "id" as id,
    "in_process_quantity" as in_process_quantity,
    "last_updated_date" as last_updated_date,
    "marketplace_id" as marketplace_id,
    "order_id" as order_id,
    "order_source" as order_source,
    "order_status" as order_status,
    "order_type" as order_type,
    "removal_fee" as removal_fee,
    "request_date" as request_date,
    "requested_quantity" as requested_quantity,
    "shipped_quantity" as shipped_quantity,
    "sku" as sku,
    "updated_at" as updated_at,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source