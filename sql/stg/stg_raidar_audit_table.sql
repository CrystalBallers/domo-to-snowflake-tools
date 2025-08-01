with
    source as (select * from {{ source("TEMP_ARGO_RAW", "RAIDAR_AUDIT_TABLE") }})

select
    "absolute_min_price" as absolute_min_price,
    "allow_dpba" as allow_dpba,
    "amazon_seller_id" as amazon_seller_id,
    "asin" as asin,
    "condition" as condition,
    "country_id" as country_id,
    "created_at" as created_at,
    "created_by" as created_by,
    "deleted_at" as deleted_at,
    "deleted_by" as deleted_by,
    "end_date" as end_date,
    "enforce_cpt" as enforce_cpt,
    "fulfillment_type" as fulfillment_type,
    "id" as id,
    "kick_start_amount" as kick_start_amount,
    "list_price" as list_price,
    "schedule_type" as schedule_type,
    "sku" as sku,
    "start_date" as start_date,
    "strategy_id" as strategy_id,
    "updated_at" as updated_at,
    "updated_by" as updated_by,
    "user_max_price" as user_max_price,
    "vendor_credit" as vendor_credit,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source