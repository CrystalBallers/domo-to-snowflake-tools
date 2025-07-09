/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 120
Dataflow Name: Velocities by ASIN
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 11
  Successful: 11
  Failed: 0
  Unique Action Types: 7
  Action Types: Filter, GroupBy, LoadFromVault, MergeJoin, PublishToVault, SelectValues, Unique
  Pipelines: 3

Generated: 2025-07-02 10:25:05
================================================================================
*/

WITH _Demand_Forecast AS (
  SELECT `amazon_seller_id`, `date`, `i_1`, `i_2`, `i_3`, `i_4`, `i_5`, `id`, `o_1`, `o_2`, `o_3`, `o_4`, `o_5`, `sku`, `v_1`, `v_2`, `v_3`, `v_4`, `v_5`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM Demand_Forecast
),

_View_of_Products_w_Categories_v2 AS (
  SELECT `amazon_asin_id`, `amazon_seller_id`, `asin`, `loreal_media_pillar_goals`, `asin_active`, `loreal_media_categories`, `brand_id`, `loreal_class_media`, `client_id`, `loreal_franchise`, `cost`, `deleted_at`, `loreal_division`, `fnsku`, `loreal_axe_media`, `id`, `loreal_sub_axe_media`, `is_bp_enforceable`, `loreal_hero_cmo_mapping`, `is_bundle`, `marketplace_id`, `name`, `parent_variation`, `reorder_status`, `sku`, `test_merge`, `units_in_listing`, `upc`, `upc_id`, `upc_name`, `vendor_parameter_id`, `vendor_sku`, `main_category`, `sub_category_1`, `sub_category_2`, `sub_category_3`, `srp`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM View_of_Products_w_Categories_v2
),

_Brands AS (
  SELECT `amazon_seller_id`, `baseline_date`, `client_id`, `created_at`, `created_by`, `currency`, `deleted_at`, `deleted_by`, `enabled_executive_report`, `enabled_projections`, `estimated_anual_sales`, `growth_target`, `id`, `legacy_brand_id`, `map_formula`, `name`, `start_date`, `updated_at`, `updated_by`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM Brands
),

_Join_Data AS (
  SELECT _View_of_Products_w_Categories_v2.`amazon_asin_id`, _View_of_Products_w_Categories_v2.`amazon_seller_id`, _View_of_Products_w_Categories_v2.`asin`, _View_of_Products_w_Categories_v2.`loreal_media_pillar_goals`, _View_of_Products_w_Categories_v2.`asin_active`, _View_of_Products_w_Categories_v2.`loreal_media_categories`, _View_of_Products_w_Categories_v2.`brand_id`, _View_of_Products_w_Categories_v2.`loreal_class_media`, _View_of_Products_w_Categories_v2.`client_id`, _View_of_Products_w_Categories_v2.`loreal_franchise`, _View_of_Products_w_Categories_v2.`cost`, _View_of_Products_w_Categories_v2.`deleted_at`, _View_of_Products_w_Categories_v2.`loreal_division`, _View_of_Products_w_Categories_v2.`fnsku`, _View_of_Products_w_Categories_v2.`loreal_axe_media`, _View_of_Products_w_Categories_v2.`id` AS `Products w/ Categories.id`, _View_of_Products_w_Categories_v2.`loreal_sub_axe_media`, _View_of_Products_w_Categories_v2.`is_bp_enforceable`, _View_of_Products_w_Categories_v2.`loreal_hero_cmo_mapping`, _View_of_Products_w_Categories_v2.`is_bundle`, _View_of_Products_w_Categories_v2.`marketplace_id`, _View_of_Products_w_Categories_v2.`name`, _View_of_Products_w_Categories_v2.`parent_variation`, _View_of_Products_w_Categories_v2.`reorder_status`, _View_of_Products_w_Categories_v2.`test_merge`, _View_of_Products_w_Categories_v2.`units_in_listing`, _View_of_Products_w_Categories_v2.`upc`, _View_of_Products_w_Categories_v2.`upc_id`, _View_of_Products_w_Categories_v2.`upc_name`, _View_of_Products_w_Categories_v2.`vendor_parameter_id`, _View_of_Products_w_Categories_v2.`vendor_sku`, _View_of_Products_w_Categories_v2.`main_category`, _View_of_Products_w_Categories_v2.`sub_category_1`, _View_of_Products_w_Categories_v2.`sub_category_2`, _View_of_Products_w_Categories_v2.`sub_category_3`, _View_of_Products_w_Categories_v2.`srp`, _View_of_Products_w_Categories_v2.`_BATCH_ID_` AS `Products w/ Categories._BATCH_ID_`, _View_of_Products_w_Categories_v2.`_BATCH_LAST_RUN_` AS `Products w/ Categories._BATCH_LAST_RUN_`, _Demand_Forecast.`date`, _Demand_Forecast.`i_1`, _Demand_Forecast.`i_2`, _Demand_Forecast.`i_3`, _Demand_Forecast.`i_4`, _Demand_Forecast.`i_5`, _Demand_Forecast.`id`, _Demand_Forecast.`o_1`, _Demand_Forecast.`o_2`, _Demand_Forecast.`o_3`, _Demand_Forecast.`o_4`, _Demand_Forecast.`o_5`, _Demand_Forecast.`sku`, _Demand_Forecast.`v_1`, _Demand_Forecast.`v_2`, _Demand_Forecast.`v_3`, _Demand_Forecast.`v_4`, _Demand_Forecast.`v_5`, _Demand_Forecast.`_BATCH_ID_`, _Demand_Forecast.`_BATCH_LAST_RUN_` FROM _View_of_Products_w_Categories_v2 INNER JOIN _Demand_Forecast ON _View_of_Products_w_Categories_v2.`sku` = _Demand_Forecast.`sku` AND _View_of_Products_w_Categories_v2.`amazon_seller_id` = _Demand_Forecast.`amazon_seller_id`
),

_Join_Data_1 AS (
  SELECT _Brands.`amazon_seller_id`, _Brands.`baseline_date`, _Brands.`client_id`, _Brands.`created_at`, _Brands.`created_by`, _Brands.`currency`, _Brands.`deleted_at`, _Brands.`deleted_by`, _Brands.`enabled_executive_report`, _Brands.`enabled_projections`, _Brands.`estimated_anual_sales`, _Brands.`growth_target`, _Brands.`id`, _Brands.`legacy_brand_id`, _Brands.`map_formula`, _Brands.`name`, _Brands.`start_date`, _Brands.`updated_at`, _Brands.`updated_by`, _Brands.`_BATCH_ID_`, _Brands.`_BATCH_LAST_RUN_`, _Join_Data.`amazon_asin_id`, _Join_Data.`amazon_seller_id` AS `Join Data.amazon_seller_id`, _Join_Data.`asin`, _Join_Data.`loreal_media_pillar_goals`, _Join_Data.`asin_active`, _Join_Data.`loreal_media_categories`, _Join_Data.`brand_id`, _Join_Data.`loreal_class_media`, _Join_Data.`client_id` AS `Join Data.client_id`, _Join_Data.`loreal_franchise`, _Join_Data.`cost`, _Join_Data.`deleted_at` AS `Join Data.deleted_at`, _Join_Data.`loreal_division`, _Join_Data.`fnsku`, _Join_Data.`loreal_axe_media`, _Join_Data.`Products w/ Categories.id`, _Join_Data.`loreal_sub_axe_media`, _Join_Data.`is_bp_enforceable`, _Join_Data.`loreal_hero_cmo_mapping`, _Join_Data.`is_bundle`, _Join_Data.`marketplace_id`, _Join_Data.`name` AS `Join Data.name`, _Join_Data.`parent_variation`, _Join_Data.`reorder_status`, _Join_Data.`test_merge`, _Join_Data.`units_in_listing`, _Join_Data.`upc`, _Join_Data.`upc_id`, _Join_Data.`upc_name`, _Join_Data.`vendor_parameter_id`, _Join_Data.`vendor_sku`, _Join_Data.`main_category`, _Join_Data.`sub_category_1`, _Join_Data.`sub_category_2`, _Join_Data.`sub_category_3`, _Join_Data.`srp`, _Join_Data.`Products w/ Categories._BATCH_ID_`, _Join_Data.`Products w/ Categories._BATCH_LAST_RUN_`, _Join_Data.`date`, _Join_Data.`i_1`, _Join_Data.`i_2`, _Join_Data.`i_3`, _Join_Data.`i_4`, _Join_Data.`i_5`, _Join_Data.`id` AS `Join Data.id`, _Join_Data.`o_1`, _Join_Data.`o_2`, _Join_Data.`o_3`, _Join_Data.`o_4`, _Join_Data.`o_5`, _Join_Data.`sku`, _Join_Data.`v_1`, _Join_Data.`v_2`, _Join_Data.`v_3`, _Join_Data.`v_4`, _Join_Data.`v_5`, _Join_Data.`_BATCH_ID_` AS `Join Data._BATCH_ID_`, _Join_Data.`_BATCH_LAST_RUN_` AS `Join Data._BATCH_LAST_RUN_` FROM _Brands INNER JOIN _Join_Data ON _Brands.`id` = _Join_Data.`brand_id`
),

_Select_Columns AS (
  SELECT `asin`, `upc`, `upc_name`, `v_1`, `v_2`, `v_3`, `v_4`, `v_5`, `date`, `name` AS `Brand`, `upc_id`, `amazon_seller_id`, `sku` FROM _Join_Data_1
),

_Filter_Rows AS (
  SELECT * FROM _Select_Columns WHERE `date` = NULL
),

_Group_By AS (
  SELECT `asin`, `upc`, `upc_name`, `date`, `Brand`, `upc_id`, AVG(`v_1`) AS `v_1`, AVG(`v_2`) AS `v_2`, AVG(`v_3`) AS `v_3`, AVG(`v_4`) AS `v_4`, AVG(`v_5`) AS `v_5` FROM _Filter_Rows GROUP BY `asin`, `upc`, `upc_name`, `date`, `Brand`, `upc_id`
),

_Remove_Duplicates AS (
  SELECT DISTINCT * FROM _Filter_Rows
),

_Velocities_by_ASIN AS (
  SELECT * FROM _Group_By
),

_Velocities_by_SKU AS (
  SELECT * FROM _Remove_Duplicates
)

SELECT * FROM _Velocities_by_SKU