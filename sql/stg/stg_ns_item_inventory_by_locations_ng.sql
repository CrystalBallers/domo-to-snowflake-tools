with
    source as (select * from {{ source("TEMP_ARGO_RAW", "NS_ITEM_INVENTORY_BY_LOCATIONS_NG") }})

select
    "Internal ID" as internal_id,
    "Name" as name,
    "Display Name" as display_name,
    "Type" as type,
    "Average Cost" as average_cost,
    "Last Purchase Price" as last_purchase_price,
    "Location On Hand" as location_on_hand,
    "Location Available" as location_available,
    "Location Committed" as location_committed,
    "Location Back Ordered" as location_back_ordered,
    "Location On Order" as location_on_order,
    "Location In Transit" as location_in_transit,
    "Location Average Cost" as location_average_cost,
    "Inventory Location" as inventory_location,
    "Location ID" as location_id,
    "Partner" as partner,
    "id" as id,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source