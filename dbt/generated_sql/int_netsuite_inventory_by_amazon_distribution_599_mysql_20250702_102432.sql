/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 599
Dataflow Name: Netsuite Inventory by Amazon Distribution
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 5
  Successful: 5
  Failed: 0
  Unique Action Types: 3
  Action Types: LoadFromVault, PublishToVault, PythonEngineAction
  Pipelines: 3

Generated: 2025-07-02 10:24:33
================================================================================
*/

WITH _product_upcs AS (
  SELECT `cost`, `deleted_at`, `id`, `name`, `old_product_id`, `product_id`, `units_in_listing`, `upc`, `upc_id`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM product_upcs
),

_amazon_skus AS (
  SELECT `amazon_asin_id`, `amazon_seller_id`, `asin`, `amazon_1`, `brand_id`, `catalog_last_appearance`, `client_id`, `comments`, `country_id`, `created_at`, `created_by`, `date_of_first_sale`, `date_of_last_sale`, `deleted_at`, `deleted_by`, `fnsku`, `fulfillment_channel`, `id`, `is_bundle`, `last_referral_fee`, `last_seen_price`, `last_shipping_fee`, `map`, `max_reorder_daily_velocity`, `min_order_qty`, `min_reorder_daily_velocity`, `name`, `old_sku`, `parent_variation`, `reorder_comments`, `reorder_status`, `sku`, `srp`, `status`, `test_merge`, `units_in_listing`, `upc`, `upc_id`, `updated_at`, `updated_by`, `vendor_parameter_id`, `is_preferred_sku_by_asin`, `is_relist_authorized`, `is_inbounding_authorized`, `is_delete_once_sold_out`, `is_agency`, `is_dangerous_good`, `is_apparel`, `is_certified_renewed`, `lithium_battery_fee`, `velocity`, `primary_velocity`, `discontinued_reason_id`, `not_sellable_reason`, `is_virtual_tracking`, `transparency_label_type_id`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM amazon_skus
),

_ns_local_inventory_by_sku AS (
  SELECT `asin`, `created_at`, `id`, `is_assembly`, `local_sku_inventory`, `local_upc_inventory`, `ns_upc_inventory`, `past_sales_sku_qty`, `past_sales_upc_qty`, `sku`, `total_past_sales_upc`, `units_in_listing`, `upc`, `upc_percentage`, `updated_at`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM ns_local_inventory_by_sku
),

_Python_Script_for_Local_Inventory AS (
  SELECT CAST(NULL AS VARCHAR) AS `asin`, CAST(NULL AS VARCHAR) AS `sku`, CAST(NULL AS DECIMAL) AS `units_in_listing`, CAST(NULL AS SIGNED) AS `is_bundle`, CAST(NULL AS DECIMAL) AS `local_sku_inventory`, CAST(NULL AS DECIMAL) AS `local_upc_inventory`, CAST(NULL AS DECIMAL) AS `ns_upc_inventory`, CAST(NULL AS DECIMAL) AS `past_sales_sku_qty`, CAST(NULL AS DECIMAL) AS `past_sales_upc_qty`, CAST(NULL AS DECIMAL) AS `total_past_sales_upc`, CAST(NULL AS VARCHAR) AS `upc`, CAST(NULL AS SIGNED) AS `id` FROM _product_upcs /* PYTHON ENGINE ACTION 67ac5a45-c4ff-4181-8b77-57a1368c8ff2 -- WARNING: This action uses Python code that cannot be directly translated to SQL -- The following Python script was executed in the original dataflow: -- -- # Import the domomagic package into the script from domomagic import * from pandas import * # This function is used to join all unique items and separate them by comma. def join_unique(x): return ', '.join(set(x.dropna().astype(str))) pu_raw = read_dataframe('product_upcs') ask_raw = read_dataframe('amazon_skus') nslocal_raw = read_dataframe('ns_local_inventory_by_sku') # Get only Amazon Skus filtered rows ask_filtered = ask_raw[ (ask_raw['deleted_at'].isnull()) & (ask_raw['status'] != 'deleted') & (ask_raw['country_id'].notnull()) ] ask_filtered = ask_filtered[['id', 'sku', 'asin', 'upc', 'units_in_listing', 'is_bundle', 'amazon_seller_id', 'country_id']] # Get only componentes: pu_filtered = pu_raw[ (pu_raw['deleted_at'].isnull()) ] pu_filtered = pu_filtered[['product_id', 'upc']] #Merge Offers with components offers_and_components_df = pandas.merge(ask_filtered, pu_filtered, left_on='id', right_on='product_id', how='left') # Optionally, drop the redundant 'product_id' column offers_and_components_df = offers_and_components_df.drop('product_id', axis=1) # Restore the upc column with upc_x and if is null use upc_y offers_and_components_df['upc'] = offers_and_components_df['upc_x'].fillna(offers_and_components_df['upc_y']) offers_and_components_df['upc_control'] = offers_and_components_df['upc_x'].fillna(offers_and_components_df['upc_y']).astype(str).str.upper().str.replace(' ', '', regex=False) offers_and_components_df['sku_control'] = offers_and_components_df['sku'].str.upper().str.replace(' ', '', regex=False) offers_and_components_df = offers_and_components_df.drop(columns=['upc_x', 'upc_y']) # Filter only sales that we need nslocal_filtered = nslocal_raw[ (nslocal_raw['is_assembly'] == 'false') ] nslocal_filtered['sku_control'] = nslocal_filtered['sku'].str.upper() nslocal_filtered['upc_control'] = nslocal_filtered['upc'].astype(str).str.upper() nslocal_filtered.info() # Merge Sales with offers and components df ns_local_df = pandas.merge(offers_and_components_df, nslocal_filtered, on=['sku_control', 'upc_control'], how='left') ns_local_df['id'] = ns_local_df['id_x'].fillna(0) ns_local_df['asin'] = ns_local_df['asin_x'].fillna(ns_local_df['asin_y']) ns_local_df['sku'] = ns_local_df['sku_x'].fillna(ns_local_df['sku_y']) ns_local_df['upc'] = ns_local_df['upc_x'].fillna(ns_local_df['upc_y']) ns_local_df['units_in_listing'] = ns_local_df['units_in_listing_x'].fillna(ns_local_df['units_in_listing_y']) ns_local_df = ns_local_df.drop(columns=['asin_x', 'asin_y', 'sku_x', 'sku_y', 'upc_x', 'upc_y', 'units_in_listing_x', 'units_in_listing_y', '_BATCH_ID_', '_BATCH_LAST_RUN_', 'id_x', 'id_y']) # Setting certain columns to zero after merge # List of columns you want to fill with 0 if null columns_to_fill = ['local_sku_inventory', 'local_upc_inventory', 'ns_upc_inventory', 'past_sales_sku_qty', 'past_sales_upc_qty', 'total_past_sales_upc', 'units_in_listing', 'upc_percentage'] ns_local_df[columns_to_fill] = ns_local_df[columns_to_fill].fillna(0) result_df = ns_local_df.groupby(['asin', 'sku', 'units_in_listing', 'is_bundle']).agg({ 'local_sku_inventory': 'min', 'local_upc_inventory': 'min', 'ns_upc_inventory': 'min', 'past_sales_sku_qty': 'min', 'past_sales_upc_qty': 'min', 'total_past_sales_upc': 'min', 'upc': join_unique,  # This will join all unique UPCs for each SKU 'id': 'first' }).reset_index() # write a data frame so it's available to the next action write_dataframe(result_df) #write_dataframe(offers_and_components_df) -- -- END PYTHON ENGINE ACTION */
),

_NS_Local_Inventory_by_SKU AS (
  SELECT * FROM _Python_Script_for_Local_Inventory
)

SELECT * FROM _NS_Local_Inventory_by_SKU