with
    source as (select * from {{ source("TEMP_ARGO_RAW", "NS_NGCOGS_2025_YTD") }})

select
    "Month" as month,
    "Type" as type,
    "Account" as account,
    "Class" as class,
    "Quantity" as quantity,
    "Amount" as amount,
    "Item" as item,
    "Internal ID" as internal_id,
    "Item Type" as item_type,
    "Document # (NRG)" as document__nrg,
    "Created From (NRG)" as created_from_nrg,
    "Vendor Name" as vendor_name,
    "id" as id,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source