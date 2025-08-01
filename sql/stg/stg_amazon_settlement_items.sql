with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_SETTLEMENT_ITEMS") }})

select
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_,
    "adjustment_id" as adjustment_id,
    "amazon_seller_id" as amazon_seller_id,
    "amount" as amount,
    "amount_description" as amount_description,
    "amount_type" as amount_type,
    "asin" as asin,
    "buyer_email" as buyer_email,
    "created_at" as created_at,
    "fulfillment_id" as fulfillment_id,
    "id" as id,
    "marketplace_name" as marketplace_name,
    "merchant_adjustment_item_id" as merchant_adjustment_item_id,
    "merchant_order_id" as merchant_order_id,
    "merchant_order_item_id" as merchant_order_item_id,
    "old_product_id" as old_product_id,
    "order_id" as order_id,
    "order_item_code" as order_item_code,
    "posted_date" as posted_date,
    "posted_date_time" as posted_date_time,
    "product_id" as product_id,
    "promotion_id" as promotion_id,
    "quantity_purchased" as quantity_purchased,
    "seller_id" as seller_id,
    "settlement_id" as settlement_id,
    "shipment_id" as shipment_id,
    "sku" as sku,
    "transaction_type" as transaction_type,
    "updated_at" as updated_at

from source