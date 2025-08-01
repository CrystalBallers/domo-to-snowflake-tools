with
    source as (select * from {{ source("TEMP_ARGO_RAW", "EXCLUDE_FROM_NS_VID_OFFERSXLSX") }})

select
    "amazon_seller_id" as amazon_seller_id,
    "country_id" as country_id,
    "msku" as msku,
    "Exclude" as exclude

from source