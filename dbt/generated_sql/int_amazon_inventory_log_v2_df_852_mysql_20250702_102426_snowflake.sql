/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 852
Dataflow Name: amazon_inventory_log_v2 DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 8
  Successful: 8
  Failed: 0
  Unique Action Types: 7
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, PublishToVault, UnionAll

Generated: 2025-07-02 10:24:26
================================================================================
*/
WITH _amazon_inventory_log AS (
  SELECT
    po_approved,
    po_created,
    po_invoiced,
    po_processed,
    po_received,
    afn_fulfillable_quantity,
    afn_researching_quantity,
    afn_unsellable_quantity,
    amazon_seller_id,
    asin,
    created_at,
    date,
    id,
    inbound_checked_in,
    inbound_delivered,
    inbound_in_transit,
    inbound_receiving,
    inbound_shipped,
    inbound_working,
    inv_age_0_to_90_days,
    inv_age_91_to_180_days,
    inv_age_181_to_270_days,
    inv_age_271_to_365_days,
    inv_age_365_plus_days,
    inv_1,
    local_sku_inventory,
    po_sku_inventory,
    qty_with_removals_in_progress,
    reserved_customerorders,
    reserved_fc_processing,
    reserved_fc_transfers,
    sku,
    updated_at,
    amazon_inventory_log_country,
    nonsellable_sku_inventory,
    exempted_from_low_inventory_level_fee,
    historical_days_of_supply,
    low_inventory_level_fee_applied_in_current_week,
    exempted_1,
    historical_1,
    low_1,
    replenishment_plan,
    batch_id,
    batch_last_run
  FROM amazon_inventory_log
), _Mission AS (
  SELECT
    *
  FROM _amazon_inventory_log
  WHERE
    amazon_seller_id = '23'
), _Everything_Else AS (
  SELECT
    *
  FROM _amazon_inventory_log
  WHERE
    amazon_seller_id <> '23'
), _Flatten_Inv_metrics AS (
  SELECT
    asin,
    date,
    amazon_seller_id,
    MAX(po_approved) AS po_approved,
    MAX(po_created) AS po_created,
    MAX(po_invoiced) AS po_invoiced,
    MAX(po_processed) AS po_processed,
    MAX(po_received) AS po_received,
    MAX(afn_fulfillable_quantity) AS afn_fulfillable_quantity,
    MAX(afn_researching_quantity) AS afn_researching_quantity,
    MAX(afn_unsellable_quantity) AS afn_unsellable_quantity,
    MAX(inbound_checked_in) AS inbound_checked_in,
    MAX(inbound_delivered) AS inbound_delivered,
    MAX(inbound_in_transit) AS inbound_in_transit,
    MAX(inbound_receiving) AS inbound_receiving,
    MAX(inbound_shipped) AS inbound_shipped,
    MAX(inbound_working) AS inbound_working,
    MAX(inv_age_0_to_90_days) AS inv_age_0_to_90_days,
    MAX(inv_age_91_to_180_days) AS inv_age_91_to_180_days,
    MAX(inv_age_181_to_270_days) AS inv_age_181_to_270_days,
    MAX(inv_age_271_to_365_days) AS inv_age_271_to_365_days,
    MAX(inv_age_365_plus_days) AS inv_age_365_plus_days,
    MAX(inv_1) AS inv_1,
    MAX(local_sku_inventory) AS local_sku_inventory,
    MAX(po_sku_inventory) AS po_sku_inventory,
    MAX(qty_with_removals_in_progress) AS qty_with_removals_in_progress,
    MAX(reserved_customerorders) AS reserved_customerorders,
    MAX(reserved_fc_processing) AS reserved_fc_processing,
    MAX(reserved_fc_transfers) AS reserved_fc_transfers,
    MIN(sku) AS sku,
    MAX(nonsellable_sku_inventory) AS nonsellable_sku_inventory
  FROM _Mission
  GROUP BY
    asin,
    date,
    amazon_seller_id
), _Get_flatten_Inv AS (
  SELECT
    _Mission.amazon_seller_id,
    _Mission.asin,
    _Mission.created_at,
    _Mission.date,
    _Mission.id,
    _Mission.sku,
    _Mission.updated_at,
    _Mission.amazon_inventory_log_country,
    _Mission.exempted_from_low_inventory_level_fee,
    _Mission.historical_days_of_supply,
    _Mission.low_inventory_level_fee_applied_in_current_week,
    _Mission.exempted_1,
    _Mission.historical_1,
    _Mission.low_1,
    _Mission.replenishment_plan,
    _Mission.batch_id,
    _Mission.batch_last_run,
    _Flatten_Inv_metrics.po_approved,
    _Flatten_Inv_metrics.po_created,
    _Flatten_Inv_metrics.po_invoiced,
    _Flatten_Inv_metrics.po_processed,
    _Flatten_Inv_metrics.po_received,
    _Flatten_Inv_metrics.afn_fulfillable_quantity,
    _Flatten_Inv_metrics.afn_researching_quantity,
    _Flatten_Inv_metrics.afn_unsellable_quantity,
    _Flatten_Inv_metrics.inbound_checked_in,
    _Flatten_Inv_metrics.inbound_delivered,
    _Flatten_Inv_metrics.inbound_in_transit,
    _Flatten_Inv_metrics.inbound_receiving,
    _Flatten_Inv_metrics.inbound_shipped,
    _Flatten_Inv_metrics.inbound_working,
    _Flatten_Inv_metrics.inv_age_0_to_90_days,
    _Flatten_Inv_metrics.inv_age_91_to_180_days,
    _Flatten_Inv_metrics.inv_age_181_to_270_days,
    _Flatten_Inv_metrics.inv_age_271_to_365_days,
    _Flatten_Inv_metrics.inv_age_365_plus_days,
    _Flatten_Inv_metrics.inv_1,
    _Flatten_Inv_metrics.local_sku_inventory,
    _Flatten_Inv_metrics.po_sku_inventory,
    _Flatten_Inv_metrics.qty_with_removals_in_progress,
    _Flatten_Inv_metrics.reserved_customerorders,
    _Flatten_Inv_metrics.reserved_fc_processing,
    _Flatten_Inv_metrics.reserved_fc_transfers,
    _Flatten_Inv_metrics.nonsellable_sku_inventory
  FROM _Mission
  LEFT JOIN _Flatten_Inv_metrics
    ON _Mission.asin = _Flatten_Inv_metrics.asin
    AND _Mission.sku = _Flatten_Inv_metrics.sku
    AND _Mission.date = _Flatten_Inv_metrics.date
), _Add_0_to_dup_rows AS (
  SELECT
    amazon_seller_id,
    asin,
    created_at,
    date,
    id,
    sku,
    updated_at,
    amazon_inventory_log_country,
    exempted_from_low_inventory_level_fee,
    historical_days_of_supply,
    low_inventory_level_fee_applied_in_current_week,
    exempted_1,
    historical_1,
    low_1,
    replenishment_plan,
    batch_id,
    batch_last_run,
    (
      COALESCE(po_approved, 0)
    ) AS po_approved,
    (
      COALESCE(po_created, 0)
    ) AS po_created,
    (
      COALESCE(po_invoiced, 0)
    ) AS po_invoiced,
    (
      COALESCE(po_processed, 0)
    ) AS po_processed,
    (
      COALESCE(po_received, 0)
    ) AS po_received,
    (
      COALESCE(afn_fulfillable_quantity, 0)
    ) AS afn_fulfillable_quantity,
    (
      COALESCE(afn_researching_quantity, 0)
    ) AS afn_researching_quantity,
    (
      COALESCE(afn_unsellable_quantity, 0)
    ) AS afn_unsellable_quantity,
    (
      COALESCE(inbound_checked_in, 0)
    ) AS inbound_checked_in,
    (
      COALESCE(inbound_delivered, 0)
    ) AS inbound_delivered,
    (
      COALESCE(inbound_in_transit, 0)
    ) AS inbound_in_transit,
    (
      COALESCE(inbound_shipped, 0)
    ) AS inbound_receiving,
    (
      COALESCE(inbound_shipped, 0)
    ) AS inbound_shipped,
    (
      COALESCE(inbound_working, 0)
    ) AS inbound_working,
    (
      COALESCE(inv_age_0_to_90_days, 0)
    ) AS inv_age_0_to_90_days,
    (
      COALESCE(inv_age_91_to_180_days, 0)
    ) AS inv_age_91_to_180_days,
    (
      COALESCE(inv_age_181_to_270_days, 0)
    ) AS inv_age_181_to_270_days,
    (
      COALESCE(inv_age_271_to_365_days, 0)
    ) AS inv_age_271_to_365_days,
    (
      COALESCE(inv_age_365_plus_days, 0)
    ) AS inv_age_365_plus_days,
    (
      COALESCE(inv_1, 0)
    ) AS inv_1,
    (
      COALESCE(local_sku_inventory, 0)
    ) AS local_sku_inventory,
    (
      COALESCE(po_sku_inventory, 0)
    ) AS po_sku_inventory,
    (
      COALESCE(qty_with_removals_in_progress, 0)
    ) AS qty_with_removals_in_progress,
    (
      COALESCE(reserved_customerorders, 0)
    ) AS reserved_customerorders,
    (
      COALESCE(reserved_fc_processing, 0)
    ) AS reserved_fc_processing,
    (
      COALESCE(reserved_fc_transfers, 0)
    ) AS reserved_fc_transfers,
    (
      COALESCE(nonsellable_sku_inventory, 0)
    ) AS nonsellable_sku_inventory
  FROM _Get_flatten_Inv
), _Clean_Inventory_removing_virtual_tracking_dups AS (
  SELECT
    po_approved,
    po_created,
    po_invoiced,
    po_processed,
    po_received,
    afn_fulfillable_quantity,
    afn_researching_quantity,
    afn_unsellable_quantity,
    amazon_seller_id,
    asin,
    created_at,
    date,
    id,
    inbound_checked_in,
    inbound_delivered,
    inbound_in_transit,
    inbound_receiving,
    inbound_shipped,
    inbound_working,
    inv_age_0_to_90_days,
    inv_age_91_to_180_days,
    inv_age_181_to_270_days,
    inv_age_271_to_365_days,
    inv_age_365_plus_days,
    inv_1,
    local_sku_inventory,
    po_sku_inventory,
    qty_with_removals_in_progress,
    reserved_customerorders,
    reserved_fc_processing,
    reserved_fc_transfers,
    sku,
    updated_at,
    amazon_inventory_log_country,
    nonsellable_sku_inventory,
    exempted_from_low_inventory_level_fee,
    historical_days_of_supply,
    low_inventory_level_fee_applied_in_current_week,
    exempted_1,
    historical_1,
    low_1,
    replenishment_plan,
    batch_id,
    batch_last_run
  FROM _Everything_Else
  UNION ALL
  SELECT
    CAST(po_approved AS DOUBLE) AS po_approved,
    CAST(po_created AS DOUBLE) AS po_created,
    CAST(po_invoiced AS DOUBLE) AS po_invoiced,
    CAST(po_processed AS DOUBLE) AS po_processed,
    CAST(po_received AS DOUBLE) AS po_received,
    CAST(afn_fulfillable_quantity AS DOUBLE) AS afn_fulfillable_quantity,
    CAST(afn_researching_quantity AS DOUBLE) AS afn_researching_quantity,
    CAST(afn_unsellable_quantity AS DOUBLE) AS afn_unsellable_quantity,
    amazon_seller_id,
    asin,
    created_at,
    date,
    id,
    CAST(inbound_checked_in AS DOUBLE) AS inbound_checked_in,
    CAST(inbound_delivered AS DOUBLE) AS inbound_delivered,
    CAST(inbound_in_transit AS DOUBLE) AS inbound_in_transit,
    CAST(inbound_receiving AS DOUBLE) AS inbound_receiving,
    CAST(inbound_shipped AS DOUBLE) AS inbound_shipped,
    CAST(inbound_working AS DOUBLE) AS inbound_working,
    CAST(inv_age_0_to_90_days AS DOUBLE) AS inv_age_0_to_90_days,
    CAST(inv_age_91_to_180_days AS DOUBLE) AS inv_age_91_to_180_days,
    CAST(inv_age_181_to_270_days AS DOUBLE) AS inv_age_181_to_270_days,
    CAST(inv_age_271_to_365_days AS DOUBLE) AS inv_age_271_to_365_days,
    CAST(inv_age_365_plus_days AS DOUBLE) AS inv_age_365_plus_days,
    CAST(inv_1 AS DOUBLE) AS inv_1,
    CAST(local_sku_inventory AS DOUBLE) AS local_sku_inventory,
    CAST(po_sku_inventory AS DOUBLE) AS po_sku_inventory,
    CAST(qty_with_removals_in_progress AS DOUBLE) AS qty_with_removals_in_progress,
    CAST(reserved_customerorders AS DOUBLE) AS reserved_customerorders,
    CAST(reserved_fc_processing AS DOUBLE) AS reserved_fc_processing,
    CAST(reserved_fc_transfers AS DOUBLE) AS reserved_fc_transfers,
    sku,
    updated_at,
    amazon_inventory_log_country,
    CAST(nonsellable_sku_inventory AS DOUBLE) AS nonsellable_sku_inventory,
    exempted_from_low_inventory_level_fee,
    historical_days_of_supply,
    low_inventory_level_fee_applied_in_current_week,
    exempted_1,
    historical_1,
    low_1,
    replenishment_plan,
    batch_id,
    batch_last_run
  FROM _Add_0_to_dup_rows
), _amazon_inventory_log_v2 AS (
  SELECT
    *
  FROM _Clean_Inventory_removing_virtual_tracking_dups
)
SELECT
  *
FROM _amazon_inventory_log_v2