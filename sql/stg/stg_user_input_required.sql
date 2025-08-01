with
    source as (select * from {{ source("TEMP_ARGO_RAW", "USER_INPUT_REQUIRED") }})

select
    "clients.name" as clientsname,
    "amazon_seller_id" as amazon_seller_id,
    "country_id" as country_id,
    "sku" as sku,
    "USER.Reporting Category" as userreporting_category,
    "USER.Pricing_Notes" as userpricing_notes,
    "USER.enforce_CM%" as userenforce_cm,
    "USER.Product Group" as userproduct_group,
    "USER.unit-of-weight" as userunitofweight,
    "USER.Package Weight" as userpackage_weight,
    "USER.unit-of-dimension" as userunitofdimension,
    "USER.Package Height" as userpackage_height,
    "USER.Package Length" as userpackage_length,
    "USER.Package Width" as userpackage_width

from source