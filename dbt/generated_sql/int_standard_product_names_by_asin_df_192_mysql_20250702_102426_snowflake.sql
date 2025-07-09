/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 192
Dataflow Name: Standard Product names by ASIN DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 12
  Successful: 12
  Failed: 0
  Unique Action Types: 7
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, PublishToVault, SelectValues
  Pipelines: 3

Generated: 2025-07-02 10:24:27
================================================================================
*/
WITH _amazon_skus AS (
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
), _vw_products_ASIN_UPC_SKU_BUNDLE AS (
  SELECT
    amazon_asin_id,
    amazon_seller_id,
    asin,
    asin_active,
    brand_id,
    client_id,
    cost,
    deleted_at,
    fnsku,
    id,
    is_bp_enforceable,
    is_bundle,
    marketplace_id,
    name,
    parent_variation,
    reorder_status,
    sku,
    test_merge,
    units_in_listing,
    upc,
    upc_id,
    upc_name,
    vendor_parameter_id,
    vendor_sku,
    batch_id,
    batch_last_run
  FROM vw_products_ASIN_UPC_SKU_BUNDLE
), _UPCs AS (
  SELECT
    brand_id,
    cases_per_layer,
    cases_per_pallet,
    catalog_source,
    client_id,
    cost,
    created_at,
    created_by,
    default_cost,
    default_discount,
    deleted_at,
    deleted_by,
    id,
    hero,
    map,
    min_order_qty,
    name,
    old_upc,
    parent_upc,
    reorder_comments,
    reorder_status,
    srp,
    unit_lxwxh_inches,
    unit_weight_lb,
    units_per_case,
    upc,
    updated_at,
    updated_by,
    vendor_sku,
    wholesale_price,
    is_discontinued_status,
    amazon_seller_id,
    unit_count_type_id,
    unit_count,
    batch_id,
    batch_last_run
  FROM UPCs
), _Remove_bundles AS (
  SELECT
    *
  FROM _vw_products_ASIN_UPC_SKU_BUNDLE
  WHERE
    is_bundle = 0 OR (
      is_bundle = 1 AND upc_name <> ''
    )
), _Get_UPC_SRP AS (
  SELECT
    _Remove_bundles.amazon_asin_id,
    _Remove_bundles.amazon_seller_id,
    _Remove_bundles.asin,
    _Remove_bundles.asin_active,
    _Remove_bundles.brand_id,
    _Remove_bundles.client_id,
    _Remove_bundles.cost,
    _Remove_bundles.deleted_at,
    _Remove_bundles.fnsku,
    _Remove_bundles.id,
    _Remove_bundles.is_bp_enforceable,
    _Remove_bundles.is_bundle,
    _Remove_bundles.marketplace_id,
    _Remove_bundles.name,
    _Remove_bundles.parent_variation,
    _Remove_bundles.reorder_status,
    _Remove_bundles.sku,
    _Remove_bundles.test_merge,
    _Remove_bundles.units_in_listing,
    _Remove_bundles.upc,
    _Remove_bundles.upc_id,
    _Remove_bundles.upc_name,
    _Remove_bundles.vendor_parameter_id,
    _Remove_bundles.vendor_sku,
    _Remove_bundles.batch_id,
    _Remove_bundles.batch_last_run,
    _UPCs.cases_per_layer,
    _UPCs.cases_per_pallet,
    _UPCs.catalog_source,
    _UPCs.created_at,
    _UPCs.created_by,
    _UPCs.default_cost,
    _UPCs.default_discount,
    _UPCs.deleted_by,
    _UPCs.hero,
    _UPCs.map,
    _UPCs.min_order_qty,
    _UPCs.old_upc,
    _UPCs.parent_upc,
    _UPCs.reorder_comments,
    _UPCs.srp AS upc_srp,
    _UPCs.unit_lxwxh_inches,
    _UPCs.unit_weight_lb,
    _UPCs.units_per_case,
    _UPCs.updated_at,
    _UPCs.updated_by,
    _UPCs.wholesale_price,
    _UPCs.is_discontinued_status,
    _UPCs.unit_count_type_id,
    _UPCs.unit_count,
    _UPCs.batch_id AS upcs_batch_id,
    _UPCs.batch_last_run AS upcs_batch_last_run
  FROM _Remove_bundles
  LEFT JOIN _UPCs
    ON _Remove_bundles.upc_id = _UPCs.id
), _Join_Data AS (
  SELECT
    _Get_UPC_SRP.amazon_asin_id,
    _Get_UPC_SRP.amazon_seller_id,
    _Get_UPC_SRP.asin,
    _Get_UPC_SRP.asin_active,
    _Get_UPC_SRP.brand_id,
    _Get_UPC_SRP.client_id,
    _Get_UPC_SRP.cost,
    _Get_UPC_SRP.deleted_at,
    _Get_UPC_SRP.fnsku,
    _Get_UPC_SRP.id,
    _Get_UPC_SRP.is_bp_enforceable,
    _Get_UPC_SRP.is_bundle,
    _Get_UPC_SRP.marketplace_id,
    _Get_UPC_SRP.name,
    _Get_UPC_SRP.parent_variation,
    _Get_UPC_SRP.reorder_status,
    _Get_UPC_SRP.sku,
    _Get_UPC_SRP.test_merge,
    _Get_UPC_SRP.units_in_listing,
    _Get_UPC_SRP.upc,
    _Get_UPC_SRP.upc_id,
    _Get_UPC_SRP.upc_name,
    _Get_UPC_SRP.vendor_parameter_id,
    _Get_UPC_SRP.vendor_sku,
    _Get_UPC_SRP.batch_id,
    _Get_UPC_SRP.batch_last_run,
    _Get_UPC_SRP.cases_per_layer,
    _Get_UPC_SRP.cases_per_pallet,
    _Get_UPC_SRP.catalog_source,
    _Get_UPC_SRP.created_at,
    _Get_UPC_SRP.created_by,
    _Get_UPC_SRP.default_cost,
    _Get_UPC_SRP.default_discount,
    _Get_UPC_SRP.deleted_by,
    _Get_UPC_SRP.hero,
    _Get_UPC_SRP.map,
    _Get_UPC_SRP.min_order_qty,
    _Get_UPC_SRP.old_upc,
    _Get_UPC_SRP.parent_upc,
    _Get_UPC_SRP.reorder_comments,
    _Get_UPC_SRP.upc_srp,
    _Get_UPC_SRP.unit_lxwxh_inches,
    _Get_UPC_SRP.unit_weight_lb,
    _Get_UPC_SRP.units_per_case,
    _Get_UPC_SRP.updated_at,
    _Get_UPC_SRP.updated_by,
    _Get_UPC_SRP.wholesale_price,
    _Get_UPC_SRP.is_discontinued_status,
    _Get_UPC_SRP.unit_count_type_id,
    _Get_UPC_SRP.unit_count,
    _Get_UPC_SRP.upcs_batch_id,
    _Get_UPC_SRP.upcs_batch_last_run,
    _amazon_skus.amazon_1,
    _amazon_skus.catalog_last_appearance,
    _amazon_skus.comments,
    _amazon_skus.country_id,
    _amazon_skus.date_of_first_sale,
    _amazon_skus.date_of_last_sale,
    _amazon_skus.fulfillment_channel,
    _amazon_skus.last_referral_fee,
    _amazon_skus.last_seen_price,
    _amazon_skus.last_shipping_fee,
    _amazon_skus.max_reorder_daily_velocity,
    _amazon_skus.min_reorder_daily_velocity,
    _amazon_skus.old_sku,
    _amazon_skus.srp,
    _amazon_skus.status,
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
    _amazon_skus.transparency_label_type_id
  FROM _Get_UPC_SRP
  LEFT JOIN _amazon_skus
    ON _Get_UPC_SRP.id = _amazon_skus.id
), _Add_Formula_1 AS (
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
    batch_last_run,
    asin_active,
    cost,
    is_bp_enforceable,
    marketplace_id,
    upc_name,
    vendor_sku,
    cases_per_layer,
    cases_per_pallet,
    catalog_source,
    default_cost,
    default_discount,
    hero,
    old_upc,
    parent_upc,
    upc_srp,
    unit_lxwxh_inches,
    unit_weight_lb,
    units_per_case,
    wholesale_price,
    is_discontinued_status,
    unit_count_type_id,
    unit_count,
    upcs_batch_id,
    upcs_batch_last_run,
    (
      upc_name
    ) AS standarized_name,
    (
      CASE WHEN is_bundle = 1 THEN srp ELSE upc_srp END
    ) AS standarized_srp
  FROM _Join_Data
), _Filter_Rows AS (
  SELECT
    *
  FROM _Add_Formula_1
  WHERE
    (
      upc <> '000000000000' AND amazon_asin_id >= '1'
    )
), _Group_By_1 AS (
  SELECT
    amazon_asin_id,
    asin,
    amazon_seller_id,
    country_id,
    standarized_name,
    MIN(standarized_srp) AS standarized_srp
  FROM _Filter_Rows
  GROUP BY
    amazon_asin_id,
    asin,
    amazon_seller_id,
    country_id,
    standarized_name
), _Group_By AS (
  SELECT
    amazon_asin_id,
    asin,
    amazon_seller_id,
    country_id,
    MIN(standarized_srp) AS standarized_srp,
    SUM(standarized_name) AS standarized_name
  FROM _Group_By_1
  GROUP BY
    amazon_asin_id,
    asin,
    amazon_seller_id,
    country_id
), _Select_Columns AS (
  SELECT
    amazon_asin_id,
    asin,
    standarized_name AS product_name,
    amazon_seller_id,
    standarized_srp AS product_srp,
    country_id
  FROM _Group_By
), _Standard_Product_names_by_ASIN_DS AS (
  SELECT
    *
  FROM _Select_Columns
)
SELECT
  *
FROM _Standard_Product_names_by_ASIN_DS