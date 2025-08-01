with
    source as (select * from {{ source("TEMP_ARGO_RAW", "NG_PRICING_LEVELS") }})

select
    "Internal ID" as internal_id,
    "Type" as type,
    "Partner" as partner,
    "Brand" as brand,
    "Name" as name,
    "Display Name" as display_name,
    "UPC Code" as upc_code,
    "Currency" as currency,
    "A - Master Cart Price" as a__master_cart_price,
    "A - Master Display Price" as a__master_display_price,
    "B - Amazon Price" as b__amazon_price,
    "B - Ebay Price" as b__ebay_price,
    "B - Newegg Price" as b__newegg_price,
    "B - Shopify Price" as b__shopify_price,
    "B - Walmart Price" as b__walmart_price,
    "B - Website Cart Price" as b__website_cart_price,
    "C - MAP Price" as c__map_price,
    "C - SRP" as c__srp,
    "C - Wholesale Price" as c__wholesale_price,
    "ZZ - Employee Price" as zz__employee_price,
    "Condition Description" as condition_description,
    "User Item Type" as user_item_type,
    "Auto price update" as auto_price_update,
    "Walmart Item Number" as walmart_item_number,
    "Walmart Item Status" as walmart_item_status,
    "Pricing Detail" as pricing_detail,
    "Pricing Group" as pricing_group,
    "Item Status" as item_status,
    "id" as id,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source