with
    source as (select * from {{ source("TEMP_ARGO_RAW", "NS_ITEM_VENDOR_MEMBERS") }})

select
    "Type" as type,
    "Internal ID" as internal_id,
    "Name" as name,
    "Display Name" as display_name,
    "Vendor" as vendor,
    "Vendor Price (Entered)" as vendor_price_entered,
    "Vendor Price Currency" as vendor_price_currency,
    "Member Item : Internal ID" as member_item__internal_id,
    "Member Item" as member_item,
    "Member Quantity" as member_quantity,
    "Member Item : Vendor" as member_item__vendor,
    "Member Item : Vendor Price (Entered)" as member_item__vendor_price_entered,
    "Member Item : Vendor Price Currency" as member_item__vendor_price_currency,
    "Vendor : Internal ID" as vendor__internal_id,
    "Vendor: ID" as vendor_id,
    "Vendor : Name" as vendor__name,
    "id" as id,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source