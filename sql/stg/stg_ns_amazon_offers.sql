with
    source as (select * from {{ source("TEMP_ARGO_RAW", "NS_AMAZON_OFFERS") }})

select
    "account" as account,
    "active" as active,
    "amazon_marketplace" as amazon_marketplace,
    "amazon_msku_status" as amazon_msku_status,
    "amazon_price" as amazon_price,
    "amazon_title" as amazon_title,
    "asin" as asin,
    "celigo_feed_inventory_override" as celigo_feed_inventory_override,
    "celigo_feed_skip_price" as celigo_feed_skip_price,
    "created_at" as created_at,
    "date_created" as date_created,
    "display_name_translated" as display_name_translated,
    "external_id" as external_id,
    "fnsku" as fnsku,
    "fulfilled_by" as fulfilled_by,
    "id" as id,
    "inactive" as inactive,
    "internal_id" as internal_id,
    "item_internal_id" as item_internal_id,
    "language" as language,
    "msku" as msku,
    "updated_at" as updated_at,
    "country_id" as country_id,
    "amazon_seller_id" as amazon_seller_id,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source