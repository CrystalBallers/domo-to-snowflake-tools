with
    source as (select * from {{ source("TEMP_ARGO_RAW", "DEFAULT_CLASS_PARTNER_BRAND_BY_VENDORXLSX") }})

select
    "Vendor" as vendor,
    "Default Class" as default_class,
    "Default Partner" as default_partner,
    "Default Brand" as default_brand

from source