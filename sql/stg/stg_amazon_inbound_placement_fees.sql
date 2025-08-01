with
    source as (select * from {{ source("TEMP_ARGO_RAW", "AMAZON_INBOUND_PLACEMENT_FEES") }})

select
    "id" as id,
    "created_at" as created_at,
    "updated_at" as updated_at,
    "amazon_seller_id" as amazon_seller_id,
    "transaction_date" as transaction_date,
    "shipping_plan_id" as shipping_plan_id,
    "fba_shipment_id" as fba_shipment_id,
    "country" as country,
    "fnsku" as fnsku,
    "asin" as asin,
    "planned_fba_inbound_placement_service" as planned_fba_inbound_placement_service,
    "planned_number_of_shipments" as planned_number_of_shipments,
    "compliant_number_of_shipments" as compliant_number_of_shipments,
    "inbound_defect_type" as inbound_defect_type,
    "actual_charge_tier" as actual_charge_tier,
    "planned_inbound_region" as planned_inbound_region,
    "actual_inbound_region" as actual_inbound_region,
    "actual_received_quantity" as actual_received_quantity,
    "product_size_tier" as product_size_tier,
    "shipping_weight" as shipping_weight,
    "unit_of_weight" as unit_of_weight,
    "fba_inbound_placement_service_fee_rate_per_unit" as fba_inbound_placement_service_fee_rate_per_unit,
    "eligible_applied_incentive" as eligible_applied_incentive,
    "currency" as currency,
    "total_fba_inbound_placement_service_fee_charge" as total_fba_inbound_placement_service_fee_charge,
    "total_charges" as total_charges,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source