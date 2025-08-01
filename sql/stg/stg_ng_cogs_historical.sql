with
    source as (select * from {{ source("TEMP_ARGO_RAW", "NG_COGS_HISTORICAL") }})

select
    "Month" as month,
    "Type" as type,
    "Account" as account,
    "Class" as class,
    "Sum of Quantity" as sum_of_quantity,
    "Sum of Amount" as sum_of_amount,
    "Item" as item,
    "Internal ID" as internal_id,
    "Item Type" as item_type,
    "Document # (NRG)" as document__nrg,
    "Created From (NRG)" as created_from_nrg,
    "Vendor Name" as vendor_name

from source