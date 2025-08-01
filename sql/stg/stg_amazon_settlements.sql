with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_SETTLEMENTS") }})

select
    "amazon_seller_id" as amazon_seller_id,
    "created_at" as created_at,
    "currency" as currency,
    "deposit_date" as deposit_date,
    "id" as id,
    "settlement_end_date" as settlement_end_date,
    "settlement_id" as settlement_id,
    "settlement_start_date" as settlement_start_date,
    "total_amount" as total_amount,
    "updated_at" as updated_at,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source