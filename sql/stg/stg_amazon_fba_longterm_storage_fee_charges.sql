with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_FBA_LONGTERM_STORAGE_FEE_CHARGES") }})

select
    "amazon_seller_id" as amazon_seller_id,
    "amount_charged" as amount_charged,
    "asin" as asin,
    "condition" as condition,
    "country" as country,
    "created_at" as created_at,
    "currency" as currency,
    "fnsku" as fnsku,
    "id" as id,
    "marketplace_id" as marketplace_id,
    "per_unit_volume" as per_unit_volume,
    "product_name" as product_name,
    "qty_charged" as qty_charged,
    "rate_surcharge" as rate_surcharge,
    "sku" as sku,
    "snapshot_date" as snapshot_date,
    "surcharge_age_tier" as surcharge_age_tier,
    "updated_at" as updated_at,
    "volume_unit" as volume_unit,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source