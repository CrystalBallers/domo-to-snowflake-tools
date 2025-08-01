with
    source as (select * from {{ source("TEMP_ARGO_RAW", "REPURPOSE_SETTLEMENTS_CACHE") }})

select
    "amazon_seller_id" as amazon_seller_id,
    "cost" as cost,
    "country_id" as country_id,
    "date" as date,
    "id" as id,
    "product_id" as product_id,
    "quantity_purchased" as quantity_purchased,
    "referral_fee" as referral_fee,
    "referral_fee_refund" as referral_fee_refund,
    "refund_commission" as refund_commission,
    "revenue" as revenue,
    "revenue_refund" as revenue_refund,
    "shipping_cost" as shipping_cost,
    "shipping_cost_refund" as shipping_cost_refund,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source