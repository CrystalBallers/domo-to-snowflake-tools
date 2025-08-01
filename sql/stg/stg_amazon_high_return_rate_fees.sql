with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_HIGH_RETURN_RATE_FEES") }})

select
    "id" as id,
    "created_at" as created_at,
    "updated_at" as updated_at,
    "amazon_seller_id" as amazon_seller_id,
    "asin" as asin,
    "asin_fee_category" as asin_fee_category,
    "fnsku" as fnsku,
    "product_name" as product_name,
    "longest_side" as longest_side,
    "median_side" as median_side,
    "shortest_side" as shortest_side,
    "measurement_units" as measurement_units,
    "unit_weight" as unit_weight,
    "dimensional_weight" as dimensional_weight,
    "shipping_weight" as shipping_weight,
    "weight_units" as weight_units,
    "sku_sizetier" as sku_sizetier,
    "month_of_shipment" as month_of_shipment,
    "asin_shipped_units" as asin_shipped_units,
    "asin_return_threshold_percent" as asin_return_threshold_percent,
    "asin_return_threshold_units" as asin_return_threshold_units,
    "asin_returned_units" as asin_returned_units,
    "sku_returned_units_NSP_exempted" as sku_returned_units_nsp_exempted,
    "sku_returned_units_charged" as sku_returned_units_charged,
    "sku_fee_per_unit" as sku_fee_per_unit,
    "sku_returns_fee" as sku_returns_fee,
    "month_of_charge" as month_of_charge,
    "currency" as currency,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source