/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 696
Dataflow Name: ASIN Fill Rate Generator
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 11
  Successful: 11
  Failed: 0
  Unique Action Types: 6
  Action Types: ExpressionEvaluator, GroupBy, LoadFromVault, MergeJoin, PublishToVault, SelectValues
  Pipelines: 3

Generated: 2025-07-02 10:24:18
================================================================================
*/
WITH _UPC_Last_Order_Receipt AS (
  SELECT
    item_upc_code,
    last_order_date,
    last_receipt_date
  FROM UPC_Last_Order_Receipt
), _amazon_skus AS (
  SELECT
    amazon_asin_id,
    amazon_seller_id,
    asin,
    amazon_1,
    brand_id,
    catalog_last_appearance,
    client_id,
    comments,
    country_id,
    created_at,
    created_by,
    date_of_first_sale,
    date_of_last_sale,
    deleted_at,
    deleted_by,
    fnsku,
    fulfillment_channel,
    id,
    is_bundle,
    last_referral_fee,
    last_seen_price,
    last_shipping_fee,
    map,
    max_reorder_daily_velocity,
    min_order_qty,
    min_reorder_daily_velocity,
    name,
    old_sku,
    parent_variation,
    reorder_comments,
    reorder_status,
    sku,
    srp,
    status,
    test_merge,
    units_in_listing,
    upc,
    upc_id,
    updated_at,
    updated_by,
    vendor_parameter_id,
    is_preferred_sku_by_asin,
    is_relist_authorized,
    is_inbounding_authorized,
    is_delete_once_sold_out,
    is_agency,
    is_dangerous_good,
    is_apparel,
    is_certified_renewed,
    lithium_battery_fee,
    velocity,
    primary_velocity,
    discontinued_reason_id,
    not_sellable_reason,
    is_virtual_tracking,
    transparency_label_type_id,
    batch_id,
    batch_last_run
  FROM amazon_skus
), _Fill_Rate_Aggregate AS (
  SELECT
    brand,
    upc,
    all_time_avg_fill_rate,
    all_time_qty_received,
    all_time_qty_ordered,
    l120_avg_fill_rate,
    l120_qty_received,
    l120_qty_ordered,
    l90_avg_fill_rate,
    l90_qty_received,
    l90_qty_ordered,
    l60_avg_fill_rate,
    l60_qty_received,
    l60_qty_ordered,
    partner
  FROM Fill_Rate_Aggregate
), _Join_Data_6 AS (
  SELECT
    _Fill_Rate_Aggregate.brand,
    _Fill_Rate_Aggregate.upc,
    _Fill_Rate_Aggregate.all_time_avg_fill_rate,
    _Fill_Rate_Aggregate.all_time_qty_received,
    _Fill_Rate_Aggregate.all_time_qty_ordered,
    _Fill_Rate_Aggregate.l120_avg_fill_rate,
    _Fill_Rate_Aggregate.l120_qty_received,
    _Fill_Rate_Aggregate.l120_qty_ordered,
    _Fill_Rate_Aggregate.l90_avg_fill_rate,
    _Fill_Rate_Aggregate.l90_qty_received,
    _Fill_Rate_Aggregate.l90_qty_ordered,
    _Fill_Rate_Aggregate.l60_avg_fill_rate,
    _Fill_Rate_Aggregate.l60_qty_received,
    _Fill_Rate_Aggregate.l60_qty_ordered,
    _Fill_Rate_Aggregate.partner,
    _UPC_Last_Order_Receipt.item_upc_code,
    _UPC_Last_Order_Receipt.last_order_date,
    _UPC_Last_Order_Receipt.last_receipt_date
  FROM _Fill_Rate_Aggregate
  LEFT JOIN _UPC_Last_Order_Receipt
    ON _Fill_Rate_Aggregate.upc = _UPC_Last_Order_Receipt.item_upc_code
), _UPC_Fill_Rate_1 AS (
  SELECT
    upc,
    all_time_avg_fill_rate,
    l120_avg_fill_rate,
    l90_avg_fill_rate,
    l60_avg_fill_rate,
    last_order_date,
    last_receipt_date,
    brand,
    partner
  FROM _Join_Data_6
), _Join_Data_5 AS (
  SELECT
    _UPC_Fill_Rate_1.upc,
    _UPC_Fill_Rate_1.all_time_avg_fill_rate,
    _UPC_Fill_Rate_1.l120_avg_fill_rate,
    _UPC_Fill_Rate_1.l90_avg_fill_rate,
    _UPC_Fill_Rate_1.l60_avg_fill_rate,
    _UPC_Fill_Rate_1.last_order_date,
    _UPC_Fill_Rate_1.last_receipt_date,
    _UPC_Fill_Rate_1.brand,
    _UPC_Fill_Rate_1.partner,
    _amazon_skus.amazon_asin_id,
    _amazon_skus.amazon_seller_id,
    _amazon_skus.asin,
    _amazon_skus.amazon_1,
    _amazon_skus.brand_id,
    _amazon_skus.catalog_last_appearance,
    _amazon_skus.client_id,
    _amazon_skus.comments,
    _amazon_skus.country_id,
    _amazon_skus.created_at,
    _amazon_skus.created_by,
    _amazon_skus.date_of_first_sale,
    _amazon_skus.date_of_last_sale,
    _amazon_skus.deleted_at,
    _amazon_skus.deleted_by,
    _amazon_skus.fnsku,
    _amazon_skus.fulfillment_channel,
    _amazon_skus.id,
    _amazon_skus.is_bundle,
    _amazon_skus.last_referral_fee,
    _amazon_skus.last_seen_price,
    _amazon_skus.last_shipping_fee,
    _amazon_skus.map,
    _amazon_skus.max_reorder_daily_velocity,
    _amazon_skus.min_order_qty,
    _amazon_skus.min_reorder_daily_velocity,
    _amazon_skus.name,
    _amazon_skus.old_sku,
    _amazon_skus.parent_variation,
    _amazon_skus.reorder_comments,
    _amazon_skus.reorder_status,
    _amazon_skus.sku,
    _amazon_skus.srp,
    _amazon_skus.status,
    _amazon_skus.test_merge,
    _amazon_skus.units_in_listing,
    _amazon_skus.upc,
    _amazon_skus.upc_id,
    _amazon_skus.updated_at,
    _amazon_skus.updated_by,
    _amazon_skus.vendor_parameter_id,
    _amazon_skus.is_preferred_sku_by_asin,
    _amazon_skus.is_relist_authorized,
    _amazon_skus.is_inbounding_authorized,
    _amazon_skus.is_delete_once_sold_out,
    _amazon_skus.is_agency,
    _amazon_skus.is_dangerous_good,
    _amazon_skus.is_apparel,
    _amazon_skus.is_certified_renewed,
    _amazon_skus.lithium_battery_fee,
    _amazon_skus.velocity,
    _amazon_skus.primary_velocity,
    _amazon_skus.discontinued_reason_id,
    _amazon_skus.not_sellable_reason,
    _amazon_skus.is_virtual_tracking,
    _amazon_skus.transparency_label_type_id,
    _amazon_skus.batch_id,
    _amazon_skus.batch_last_run
  FROM _UPC_Fill_Rate_1
  INNER JOIN _amazon_skus
    ON _UPC_Fill_Rate_1.upc = _amazon_skus.upc
), _Add_Formula_2 AS (
  SELECT
    upc,
    all_time_avg_fill_rate,
    last_order_date,
    last_receipt_date,
    brand,
    partner,
    amazon_asin_id,
    amazon_seller_id,
    asin,
    amazon_1,
    brand_id,
    catalog_last_appearance,
    client_id,
    comments,
    country_id,
    created_at,
    created_by,
    date_of_first_sale,
    date_of_last_sale,
    deleted_at,
    deleted_by,
    fnsku,
    fulfillment_channel,
    id,
    is_bundle,
    last_referral_fee,
    last_seen_price,
    last_shipping_fee,
    map,
    max_reorder_daily_velocity,
    min_order_qty,
    min_reorder_daily_velocity,
    name,
    old_sku,
    parent_variation,
    reorder_comments,
    reorder_status,
    sku,
    srp,
    status,
    test_merge,
    units_in_listing,
    upc,
    upc_id,
    updated_at,
    updated_by,
    vendor_parameter_id,
    is_preferred_sku_by_asin,
    is_relist_authorized,
    is_inbounding_authorized,
    is_delete_once_sold_out,
    is_agency,
    is_dangerous_good,
    is_apparel,
    is_certified_renewed,
    lithium_battery_fee,
    velocity,
    primary_velocity,
    discontinued_reason_id,
    not_sellable_reason,
    is_virtual_tracking,
    transparency_label_type_id,
    batch_id,
    batch_last_run,
    (
      COALESCE(l120_avg_fill_rate, all_time_avg_fill_rate)
    ) AS l120_avg_fill_rate,
    (
      COALESCE(
        l90_avg_fill_rate,
        COALESCE(l120_avg_fill_rate, all_time_avg_fill_rate, NULL)
      )
    ) AS l90_avg_fill_rate,
    (
      COALESCE(
        l60_avg_fill_rate,
        COALESCE(l90_avg_fill_rate, l120_avg_fill_rate, all_time_avg_fill_rate, NULL)
      )
    ) AS l60_avg_fill_rate
  FROM _Join_Data_5
), _Select_Columns_1 AS (
  SELECT
    asin,
    all_time_avg_fill_rate,
    l120_avg_fill_rate,
    l90_avg_fill_rate,
    l60_avg_fill_rate,
    upc,
    last_order_date,
    last_receipt_date,
    brand,
    partner
  FROM _Add_Formula_2
), _Group_By_1 AS (
  SELECT
    asin,
    SUM(l120_avg_fill_rate) AS l120_avg_fill_rate,
    SUM(l90_avg_fill_rate) AS l90_avg_fill_rate,
    SUM(l60_avg_fill_rate) AS l60_avg_fill_rate,
    SUM(all_time_avg_fill_rate) AS all_time_avg_fill_rate,
    MAX(last_order_date) AS last_order_date,
    MAX(last_receipt_date) AS last_receipt_date,
    MIN(brand) AS brand,
    MIN(partner) AS partner
  FROM _Select_Columns_1
  GROUP BY
    asin
), _Add_Formula_3 AS (
  SELECT
    asin,
    last_order_date,
    last_receipt_date,
    brand,
    partner,
    (
      TRUNCATE(l120_avg_fill_rate * 100, 2)
    ) AS l120_avg_fill_rate,
    (
      TRUNCATE(l90_avg_fill_rate * 100, 2)
    ) AS l90_avg_fill_rate,
    (
      TRUNCATE(l60_avg_fill_rate * 100, 2)
    ) AS l60_avg_fill_rate,
    (
      TRUNCATE(all_time_avg_fill_rate * 100, 2)
    ) AS all_time_avg_fill_rate
  FROM _Group_By_1
), _ASIN_Combined_Fill_Rate_2_0 AS (
  SELECT
    *
  FROM _Add_Formula_3
)
SELECT
  *
FROM _ASIN_Combined_Fill_Rate_2_0