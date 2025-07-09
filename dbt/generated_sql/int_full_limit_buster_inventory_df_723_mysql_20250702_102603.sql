/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 723
Dataflow Name: Full Limit Buster Inventory DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 36
  Successful: 36
  Failed: 0
  Unique Action Types: 10
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, Metadata, PublishToVault, SelectValues, Unique, ValueMapper
  Pipelines: 9

Generated: 2025-07-02 10:26:07
================================================================================
*/

WITH _amazon_seller_inventory AS (
  SELECT `id`, `asin`, `sellerName`, `sellerId`, `quantity`, `isQuantityRestricted`, `date`, `time`, `isFba`, `limitBuster`, `price`, `redirectedASIN`, `site_extension`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM amazon_seller_inventory
),

_Product_List_DataSet AS (
  SELECT `amazon_seller_id`, `asin`, `brand_id`, `client_id`, `cost`, `deleted_at`, `fnsku`, `id`, `is_bundle`, `reorder_status`, `sku`, `units_in_listing`, `upc`, `vendor_parameter_id`, `vendor_sku`, `_BATCH_ID_`, `_BATCH_LAST_RUN_`, `advertisement_enabled`, `description`, `estimated_annual_sales`, `member_since`, `partner name`, `currency`, `estimated_anual_sales`, `legacy_brand_id`, `map_formula`, `Brand`, `category`, `subcategory`, `brand_growth_target`, `msrp`, `map`, `amazon_asin_id`, `upc_id`, `country_id`, `srp`, `upc_srp`, `upc_default_cost`, `upc_default_discount`, `default_cost`, `default_discount`, `UPC_MAP`, `wholesale_price`, `parent_asin`, `catalog_source`, `country_of_origin`, `min_order_qty`, `units_per_case`, `unit_lxwxh_inches`, `unit_weight_lb`, `sku_reorder_status`, `upc_reorder_status`, `is_discontinued_status`, `upc_components`, `catalog_last_appearance`, `parent_category`, `child_category`, `child_category_3`, `child_category_2`, `sku_reorder_comments`, `upc_reorder_comments`, `is_preferred_sku_by_asin`, `seller_name`, `countries.name`, `sku created date`, `upc created date`, `sku_cost`, `is_current`, `fulfillment_channel`, `last_seen_price`, `last_known_price`, `estimated_units_sold_last_month`, `hero`, `upc_count_total_components`, `brand_created_at`, `seller_start_date`, `brand_start_date`, `amazon_seller_central_accounts_created_at`, `BP Partner Name`, `BP Brand Name`, `Partner - Brand`, `seller_created_at`, `Preferred_SKU(s) on ASIN`, `QA - Count FBM Offers (Current)`, `QA - Count FBA Offers (Current)`, `QA -Count reorder Status Yes`, `QA -Count Preferred_SKU(s)`, `QA -Current_Preferred_SKU(s)`, `is_current_ASIN`, `name`, `upc_name` FROM Product_List_DataSet
),

_View_of_UPCs_w_Categories_v2 AS (
  SELECT `brand_id`, `cases_per_layer`, `cases_per_pallet`, `loreal_media_pillar_goals`, `catalog_source`, `loreal_media_categories`, `client_id`, `loreal_class_media`, `cost`, `loreal_franchise`, `created_at`, `loreal_sub_franchise`, `created_by`, `loreal_division`, `default_cost`, `loreal_axe_media`, `default_discount`, `loreal_sub_axe_media`, `deleted_at`, `loreal_hero_cmo_mapping`, `deleted_by`, `loreal_hero_parent`, `id`, `loreal_hero`, `hero`, `map`, `min_order_qty`, `name`, `old_upc`, `parent_upc`, `reorder_comments`, `reorder_status`, `srp`, `unit_lxwxh_inches`, `unit_weight_lb`, `units_per_case`, `upc`, `updated_at`, `updated_by`, `vendor_sku`, `wholesale_price`, `is_discontinued_status`, `amazon_seller_id`, `unit_count_type_id`, `unit_count`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM View_of_UPCs_w_Categories_v2
),

_bp_brands_mapping AS (
  SELECT `brand_id`, `client_id`, `bp_partner_id`, `bp_brand_id`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM bp_brands_mapping
),

_bp_asin_enforcement_settings AS (
  SELECT `source`, `country_id`, `country_name`, `asin`, `units_in_listing`, `is_bp_enforceable`, `offers_scraper_enabled`, `suppression_tracking_enabled`, `is_hero`, `hero_configuration`, `transparency_status`, `asin_title`, `cpt`, `brand_name`, `client_name`, `buy_box_status`, `buy_box_status_clasification`, `is_minderest_tracking`, `asin_active`, `map`, `last_seen_price`, `upc`, `is_bundle`, `brand_assigned_user`, `brand_assigned_user_initials`, `recom_price`, `last_scraper_seen_date`, `asin_scraped_offers_count`, `ltm_rev`, `asin_L90_avg_sales`, `asin_L90_active_bb_avg_sales`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM bp_asin_enforcement_settings
),

_Offers_Storefronts_by_Date AS (
  SELECT `scraped_date`, `asin`, `amazonID`, `storefront_name`, `country_id`, `isFBA`, `isPrime`, `isAmazon`, `cod_condition`, `offer_date_max_price`, `offer_date_min_price`, `offer_date_avg_price`, `offer_date_count`, `buy_box_count`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM Offers_Storefronts_by_Date
),

_countries AS (
  SELECT `created_at`, `currency`, `enabled`, `id`, `marketplace_id`, `name`, `sales_channel`, `short_name`, `updated_at`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM countries
),

_BP_Gated_Status AS (
  SELECT `ASIN`, `UPC`, `Catalog Brand`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS` FROM BP_Gated_Status
),

_ASIN_Rolling_3_Month_Revenue_Rank_DPDS AS (
  SELECT `Brand`, `partner name`, `asin`, `sales_country_id`, `revenue`, `asins units`, `revenue brand`, `asins units brand`, `Percent Mix of Brand`, `Revenue Rank R3MTH`, `Revenue Distribution R3MTH` FROM ASIN_Rolling_3_Month_Revenue_Rank_DPDS
),

_Change_price_typ_to_text AS (
  SELECT CAST(`price` AS VARCHAR), `id`, `asin`, `sellerName`, `sellerId`, `quantity`, `isQuantityRestricted`, `date`, `time`, `isFba`, `limitBuster`, `redirectedASIN`, `site_extension`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM _amazon_seller_inventory
),

_brands_mapping AS (
  SELECT _Product_List_DataSet.`amazon_seller_id`, _Product_List_DataSet.`asin`, _Product_List_DataSet.`brand_id`, _Product_List_DataSet.`client_id`, _Product_List_DataSet.`cost`, _Product_List_DataSet.`deleted_at`, _Product_List_DataSet.`fnsku`, _Product_List_DataSet.`id`, _Product_List_DataSet.`is_bundle`, _Product_List_DataSet.`reorder_status`, _Product_List_DataSet.`sku`, _Product_List_DataSet.`units_in_listing`, _Product_List_DataSet.`upc`, _Product_List_DataSet.`vendor_parameter_id`, _Product_List_DataSet.`vendor_sku`, _Product_List_DataSet.`_BATCH_ID_`, _Product_List_DataSet.`_BATCH_LAST_RUN_`, _Product_List_DataSet.`advertisement_enabled`, _Product_List_DataSet.`description`, _Product_List_DataSet.`estimated_annual_sales`, _Product_List_DataSet.`member_since`, _Product_List_DataSet.`partner name`, _Product_List_DataSet.`currency`, _Product_List_DataSet.`estimated_anual_sales`, _Product_List_DataSet.`legacy_brand_id`, _Product_List_DataSet.`map_formula`, _Product_List_DataSet.`Brand`, _Product_List_DataSet.`category`, _Product_List_DataSet.`subcategory`, _Product_List_DataSet.`brand_growth_target`, _Product_List_DataSet.`msrp`, _Product_List_DataSet.`map`, _Product_List_DataSet.`amazon_asin_id`, _Product_List_DataSet.`upc_id`, _Product_List_DataSet.`country_id`, _Product_List_DataSet.`srp`, _Product_List_DataSet.`upc_srp`, _Product_List_DataSet.`upc_default_cost`, _Product_List_DataSet.`upc_default_discount`, _Product_List_DataSet.`default_cost`, _Product_List_DataSet.`default_discount`, _Product_List_DataSet.`UPC_MAP`, _Product_List_DataSet.`wholesale_price`, _Product_List_DataSet.`parent_asin`, _Product_List_DataSet.`catalog_source`, _Product_List_DataSet.`country_of_origin`, _Product_List_DataSet.`min_order_qty`, _Product_List_DataSet.`units_per_case`, _Product_List_DataSet.`unit_lxwxh_inches`, _Product_List_DataSet.`unit_weight_lb`, _Product_List_DataSet.`sku_reorder_status`, _Product_List_DataSet.`upc_reorder_status`, _Product_List_DataSet.`is_discontinued_status`, _Product_List_DataSet.`upc_components`, _Product_List_DataSet.`catalog_last_appearance`, _Product_List_DataSet.`parent_category`, _Product_List_DataSet.`child_category`, _Product_List_DataSet.`child_category_3`, _Product_List_DataSet.`child_category_2`, _Product_List_DataSet.`sku_reorder_comments`, _Product_List_DataSet.`upc_reorder_comments`, _Product_List_DataSet.`is_preferred_sku_by_asin`, _Product_List_DataSet.`seller_name`, _Product_List_DataSet.`countries.name`, _Product_List_DataSet.`sku created date`, _Product_List_DataSet.`upc created date`, _Product_List_DataSet.`sku_cost`, _Product_List_DataSet.`is_current`, _Product_List_DataSet.`fulfillment_channel`, _Product_List_DataSet.`last_seen_price`, _Product_List_DataSet.`last_known_price`, _Product_List_DataSet.`estimated_units_sold_last_month`, _Product_List_DataSet.`hero`, _Product_List_DataSet.`upc_count_total_components`, _Product_List_DataSet.`brand_created_at`, _Product_List_DataSet.`seller_start_date`, _Product_List_DataSet.`brand_start_date`, _Product_List_DataSet.`amazon_seller_central_accounts_created_at`, _Product_List_DataSet.`BP Partner Name`, _Product_List_DataSet.`BP Brand Name`, _Product_List_DataSet.`Partner - Brand`, _Product_List_DataSet.`seller_created_at`, _Product_List_DataSet.`Preferred_SKU(s) on ASIN`, _Product_List_DataSet.`QA - Count FBM Offers (Current)`, _Product_List_DataSet.`QA - Count FBA Offers (Current)`, _Product_List_DataSet.`QA -Count reorder Status Yes`, _Product_List_DataSet.`QA -Count Preferred_SKU(s)`, _Product_List_DataSet.`QA -Current_Preferred_SKU(s)`, _Product_List_DataSet.`is_current_ASIN`, _Product_List_DataSet.`name`, _Product_List_DataSet.`upc_name`, _bp_brands_mapping.`bp_partner_id`, _bp_brands_mapping.`bp_brand_id` FROM _Product_List_DataSet INNER JOIN _bp_brands_mapping ON _Product_List_DataSet.`brand_id` = _bp_brands_mapping.`brand_id` AND _Product_List_DataSet.`client_id` = _bp_brands_mapping.`client_id`
),

_Select_Columns AS (
  SELECT `asin`, `is_bp_enforceable` AS `asin_bp_enforceable` FROM _bp_asin_enforcement_settings
),

_select_name AS (
  SELECT `id`, `name` AS `country` FROM _countries
),

_Select_Gated_Columns AS (
  SELECT `ASIN`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS` FROM _BP_Gated_Status
),

_Modify_date_time_storefront_name_and_price AS (
  SELECT `id`, `asin`, `sellerName`, `sellerId`, `quantity`, `isQuantityRestricted`, `date`, `time`, `isFba`, `limitBuster`, `redirectedASIN`, `site_extension`, `_BATCH_ID_`, `_BATCH_LAST_RUN_`, (`date`) AS `created_date`, (`time`) AS `created_time`, (TRIM(`sellerName`)) AS `storefront_name`, (CASE WHEN STR_CONTAINS(`price`, '$') THEN SUBSTRING(`price`, 1, (FLOOR(CHAR_LENGTH(`price`)) / 2) + 1) ELSE `price` END) AS `price` FROM _Change_price_typ_to_text
),

_Group_By AS (
  SELECT `asin`, `brand_id`, `Brand`, `upc_name`, `msrp`, `map`, `upc`, `partner name`, `upc_id`, `country_id`, `amazon_seller_id`, `Partner - Brand`, `bp_brand_id`, COUNT(`sku`) AS `total_products`, MIN(`name`) AS `sku_name`, SUM(`units_in_listing`) AS `unit_in_listing` FROM _brands_mapping GROUP BY `asin`, `brand_id`, `Brand`, `upc_name`, `msrp`, `map`, `upc`, `partner name`, `upc_id`, `country_id`, `amazon_seller_id`, `Partner - Brand`, `bp_brand_id`
),

_Join_Data_3 AS (
  SELECT _Offers_Storefronts_by_Date.`scraped_date`, _Offers_Storefronts_by_Date.`asin`, _Offers_Storefronts_by_Date.`amazonID`, _Offers_Storefronts_by_Date.`storefront_name`, _Offers_Storefronts_by_Date.`country_id`, _Offers_Storefronts_by_Date.`isFBA`, _Offers_Storefronts_by_Date.`isPrime`, _Offers_Storefronts_by_Date.`isAmazon`, _Offers_Storefronts_by_Date.`cod_condition`, _Offers_Storefronts_by_Date.`offer_date_max_price`, _Offers_Storefronts_by_Date.`offer_date_min_price`, _Offers_Storefronts_by_Date.`offer_date_avg_price`, _Offers_Storefronts_by_Date.`offer_date_count`, _Offers_Storefronts_by_Date.`buy_box_count`, _Offers_Storefronts_by_Date.`_BATCH_ID_`, _Offers_Storefronts_by_Date.`_BATCH_LAST_RUN_`, _select_name.`country` FROM _Offers_Storefronts_by_Date INNER JOIN _select_name ON _Offers_Storefronts_by_Date.`country_id` = _select_name.`id`
),

_Add_decimal_to_price AS (
  SELECT CAST(`price` AS DECIMAL), `id`, `asin`, `sellerName`, `sellerId`, `quantity`, `isQuantityRestricted`, `date`, `time`, `isFba`, `limitBuster`, `redirectedASIN`, `site_extension`, `_BATCH_ID_`, `_BATCH_LAST_RUN_`, `created_date`, `created_time`, `storefront_name` FROM _Modify_date_time_storefront_name_and_price
),

_Join_Data_2 AS (
  SELECT _Group_By.`asin`, _Group_By.`brand_id`, _Group_By.`Brand`, _Group_By.`upc_name`, _Group_By.`msrp`, _Group_By.`map`, _Group_By.`upc`, _Group_By.`partner name`, _Group_By.`upc_id`, _Group_By.`country_id`, _Group_By.`amazon_seller_id`, _Group_By.`Partner - Brand`, _Group_By.`bp_brand_id`, _Group_By.`total_products`, _Group_By.`sku_name`, _Group_By.`unit_in_listing`, _Select_Columns.`asin_bp_enforceable` FROM _Group_By LEFT JOIN _Select_Columns ON _Group_By.`asin` = _Select_Columns.`asin`
),

_Remove_spaces_from_storefront_name AS (
  SELECT `scraped_date`, `asin`, `amazonID`, `country_id`, `isFBA`, `isPrime`, `isAmazon`, `cod_condition`, `offer_date_max_price`, `offer_date_min_price`, `offer_date_avg_price`, `offer_date_count`, `buy_box_count`, `_BATCH_ID_`, `_BATCH_LAST_RUN_`, `country`, (trim(`storefront_name`)) AS `storefront_name` FROM _Join_Data_3
),

_Remove_unnecessary_columns AS (
  SELECT `sellerName`, `storefront_name`, `sellerId`, `asin`, `isQuantityRestricted`, `isFba`, `quantity` AS `Units`, `limitBuster` AS `Limit Buster Units`, `created_time`, `created_date`, `price` AS `offer_price`, `redirectedASIN` AS `redirect_asin`, `site_extension` FROM _Add_decimal_to_price
),

_ASIN_UPC AS (
  SELECT _Join_Data_2.`asin`, _Join_Data_2.`brand_id`, _Join_Data_2.`Brand`, _Join_Data_2.`upc_name`, _Join_Data_2.`msrp`, _Join_Data_2.`map` AS `sku_map`, _Join_Data_2.`upc`, _Join_Data_2.`partner name`, _Join_Data_2.`upc_id`, _Join_Data_2.`country_id`, _Join_Data_2.`amazon_seller_id`, _Join_Data_2.`Partner - Brand`, _Join_Data_2.`bp_brand_id`, _Join_Data_2.`total_products`, _Join_Data_2.`sku_name`, _Join_Data_2.`unit_in_listing`, _Join_Data_2.`asin_bp_enforceable`, _View_of_UPCs_w_Categories_v2.`brand_id` AS `UPCs w/ Categories.brand_id`, _View_of_UPCs_w_Categories_v2.`cases_per_layer`, _View_of_UPCs_w_Categories_v2.`cases_per_pallet`, _View_of_UPCs_w_Categories_v2.`loreal_media_pillar_goals`, _View_of_UPCs_w_Categories_v2.`catalog_source`, _View_of_UPCs_w_Categories_v2.`loreal_media_categories`, _View_of_UPCs_w_Categories_v2.`client_id`, _View_of_UPCs_w_Categories_v2.`loreal_class_media`, _View_of_UPCs_w_Categories_v2.`cost`, _View_of_UPCs_w_Categories_v2.`loreal_franchise`, _View_of_UPCs_w_Categories_v2.`created_at`, _View_of_UPCs_w_Categories_v2.`loreal_sub_franchise`, _View_of_UPCs_w_Categories_v2.`created_by`, _View_of_UPCs_w_Categories_v2.`loreal_division`, _View_of_UPCs_w_Categories_v2.`default_cost`, _View_of_UPCs_w_Categories_v2.`loreal_axe_media`, _View_of_UPCs_w_Categories_v2.`default_discount`, _View_of_UPCs_w_Categories_v2.`loreal_sub_axe_media`, _View_of_UPCs_w_Categories_v2.`deleted_at`, _View_of_UPCs_w_Categories_v2.`loreal_hero_cmo_mapping`, _View_of_UPCs_w_Categories_v2.`deleted_by`, _View_of_UPCs_w_Categories_v2.`loreal_hero_parent`, _View_of_UPCs_w_Categories_v2.`id`, _View_of_UPCs_w_Categories_v2.`loreal_hero`, _View_of_UPCs_w_Categories_v2.`hero`, _View_of_UPCs_w_Categories_v2.`map`, _View_of_UPCs_w_Categories_v2.`min_order_qty`, _View_of_UPCs_w_Categories_v2.`name`, _View_of_UPCs_w_Categories_v2.`old_upc`, _View_of_UPCs_w_Categories_v2.`parent_upc`, _View_of_UPCs_w_Categories_v2.`reorder_comments`, _View_of_UPCs_w_Categories_v2.`reorder_status`, _View_of_UPCs_w_Categories_v2.`srp`, _View_of_UPCs_w_Categories_v2.`unit_lxwxh_inches`, _View_of_UPCs_w_Categories_v2.`unit_weight_lb`, _View_of_UPCs_w_Categories_v2.`units_per_case`, _View_of_UPCs_w_Categories_v2.`updated_at`, _View_of_UPCs_w_Categories_v2.`updated_by`, _View_of_UPCs_w_Categories_v2.`vendor_sku`, _View_of_UPCs_w_Categories_v2.`wholesale_price`, _View_of_UPCs_w_Categories_v2.`is_discontinued_status`, _View_of_UPCs_w_Categories_v2.`unit_count_type_id`, _View_of_UPCs_w_Categories_v2.`unit_count`, _View_of_UPCs_w_Categories_v2.`parent_category`, _View_of_UPCs_w_Categories_v2.`child_category`, _View_of_UPCs_w_Categories_v2.`child_category_2`, _View_of_UPCs_w_Categories_v2.`child_category_3`, _View_of_UPCs_w_Categories_v2.`_BATCH_ID_`, _View_of_UPCs_w_Categories_v2.`_BATCH_LAST_RUN_` FROM _Join_Data_2 LEFT JOIN _View_of_UPCs_w_Categories_v2 ON _Join_Data_2.`upc` = _View_of_UPCs_w_Categories_v2.`upc` AND _Join_Data_2.`upc_id` = _View_of_UPCs_w_Categories_v2.`id` AND _Join_Data_2.`amazon_seller_id` = _View_of_UPCs_w_Categories_v2.`amazon_seller_id`
),

_Remove_TODAY_from_Inventory AS (
  SELECT * FROM _Remove_unnecessary_columns WHERE `created_date` < NULL
),

_Country_ID AS (
  SELECT `sellerName`, `storefront_name`, `sellerId`, `asin`, `isQuantityRestricted`, `isFba`, `Units`, `Limit Buster Units`, `created_time`, `created_date`, `offer_price`, `redirect_asin`, `site_extension`, (case when `site_extension` = 'com' then 1 when `site_extension` = 'ca' then 2 when `site_extension` = 'co.uk' then 3 when `site_extension` = 'fr' then 4 when `site_extension` = 'it' then 5 when `site_extension` = 'de' then 6 when `site_extension` = 'es' then 7 when `site_extension` = 'nl' then 8 when `site_extension` = 'pl' then 9 when `site_extension` = 'se' then 10 when `site_extension` = 'be' then 11 else 0 end) AS `Country ID` FROM _Remove_TODAY_from_Inventory
),

_Join_with_all_scraped_storefronts AS (
  SELECT _Remove_spaces_from_storefront_name.`scraped_date`, _Remove_spaces_from_storefront_name.`asin`, _Remove_spaces_from_storefront_name.`amazonID`, _Remove_spaces_from_storefront_name.`storefront_name`, _Remove_spaces_from_storefront_name.`country_id`, _Remove_spaces_from_storefront_name.`isFBA`, _Remove_spaces_from_storefront_name.`isPrime`, _Remove_spaces_from_storefront_name.`isAmazon`, _Remove_spaces_from_storefront_name.`cod_condition`, _Remove_spaces_from_storefront_name.`offer_date_max_price`, _Remove_spaces_from_storefront_name.`buy_box_count`, _Remove_spaces_from_storefront_name.`_BATCH_ID_`, _Remove_spaces_from_storefront_name.`_BATCH_LAST_RUN_`, _Remove_spaces_from_storefront_name.`country`, _Country_ID.`sellerName`, _Country_ID.`sellerId`, _Country_ID.`isQuantityRestricted`, _Country_ID.`Units`, _Country_ID.`Limit Buster Units`, _Country_ID.`created_time`, _Country_ID.`created_date`, _Country_ID.`offer_price`, _Country_ID.`redirect_asin`, _Country_ID.`site_extension`, _Country_ID.`Country ID` FROM _Remove_spaces_from_storefront_name LEFT JOIN _Country_ID ON _Remove_spaces_from_storefront_name.`asin` = _Country_ID.`asin` AND _Remove_spaces_from_storefront_name.`scraped_date` = _Country_ID.`created_date` AND _Remove_spaces_from_storefront_name.`storefront_name` = _Country_ID.`storefront_name` AND _Remove_spaces_from_storefront_name.`isFBA` = _Country_ID.`isFba` AND _Remove_spaces_from_storefront_name.`country_id` = _Country_ID.`Country ID`
),

_Join_Data AS (
  SELECT _Join_with_all_scraped_storefronts.`sellerName`, _Join_with_all_scraped_storefronts.`storefront_name`, _Join_with_all_scraped_storefronts.`sellerId`, _Join_with_all_scraped_storefronts.`asin`, _Join_with_all_scraped_storefronts.`isQuantityRestricted`, _Join_with_all_scraped_storefronts.`isFba`, _Join_with_all_scraped_storefronts.`Units`, _Join_with_all_scraped_storefronts.`Limit Buster Units`, _Join_with_all_scraped_storefronts.`created_time`, _Join_with_all_scraped_storefronts.`created_date`, _Join_with_all_scraped_storefronts.`offer_price`, _Join_with_all_scraped_storefronts.`redirect_asin`, _Join_with_all_scraped_storefronts.`site_extension`, _Join_with_all_scraped_storefronts.`Country ID`, _Join_with_all_scraped_storefronts.`scraped_date`, _Join_with_all_scraped_storefronts.`amazonID`, _Join_with_all_scraped_storefronts.`country_id`, _Join_with_all_scraped_storefronts.`isFBA`, _Join_with_all_scraped_storefronts.`isPrime`, _Join_with_all_scraped_storefronts.`isAmazon`, _Join_with_all_scraped_storefronts.`cod_condition`, _Join_with_all_scraped_storefronts.`offer_date_max_price`, _Join_with_all_scraped_storefronts.`offer_date_min_price`, _Join_with_all_scraped_storefronts.`offer_date_avg_price`, _Join_with_all_scraped_storefronts.`offer_date_count`, _Join_with_all_scraped_storefronts.`buy_box_count`, _Join_with_all_scraped_storefronts.`_BATCH_ID_`, _Join_with_all_scraped_storefronts.`_BATCH_LAST_RUN_`, _Join_with_all_scraped_storefronts.`country`, _ASIN_UPC.`brand_id`, _ASIN_UPC.`cases_per_layer`, _ASIN_UPC.`cases_per_pallet`, _ASIN_UPC.`loreal_media_pillar_goals`, _ASIN_UPC.`catalog_source`, _ASIN_UPC.`loreal_media_categories`, _ASIN_UPC.`client_id`, _ASIN_UPC.`loreal_class_media`, _ASIN_UPC.`cost`, _ASIN_UPC.`loreal_franchise`, _ASIN_UPC.`created_at`, _ASIN_UPC.`loreal_sub_franchise`, _ASIN_UPC.`created_by`, _ASIN_UPC.`loreal_division`, _ASIN_UPC.`default_cost`, _ASIN_UPC.`loreal_axe_media`, _ASIN_UPC.`default_discount`, _ASIN_UPC.`loreal_sub_axe_media`, _ASIN_UPC.`deleted_at`, _ASIN_UPC.`loreal_hero_cmo_mapping`, _ASIN_UPC.`deleted_by`, _ASIN_UPC.`loreal_hero_parent`, _ASIN_UPC.`id`, _ASIN_UPC.`loreal_hero`, _ASIN_UPC.`hero`, _ASIN_UPC.`sku_map`, _ASIN_UPC.`min_order_qty`, _ASIN_UPC.`name`, _ASIN_UPC.`old_upc`, _ASIN_UPC.`parent_upc`, _ASIN_UPC.`reorder_comments`, _ASIN_UPC.`reorder_status`, _ASIN_UPC.`srp`, _ASIN_UPC.`unit_lxwxh_inches`, _ASIN_UPC.`unit_weight_lb`, _ASIN_UPC.`units_per_case`, _ASIN_UPC.`upc`, _ASIN_UPC.`updated_at`, _ASIN_UPC.`updated_by`, _ASIN_UPC.`vendor_sku`, _ASIN_UPC.`wholesale_price`, _ASIN_UPC.`is_discontinued_status`, _ASIN_UPC.`amazon_seller_id`, _ASIN_UPC.`unit_count_type_id`, _ASIN_UPC.`unit_count`, _ASIN_UPC.`parent_category`, _ASIN_UPC.`child_category`, _ASIN_UPC.`child_category_2`, _ASIN_UPC.`child_category_3`, _ASIN_UPC.`UPCs w/ Categories.brand_id`, _ASIN_UPC.`Brand`, _ASIN_UPC.`upc_name`, _ASIN_UPC.`msrp`, _ASIN_UPC.`map`, _ASIN_UPC.`partner name`, _ASIN_UPC.`upc_id`, _ASIN_UPC.`Partner - Brand`, _ASIN_UPC.`bp_brand_id`, _ASIN_UPC.`total_products`, _ASIN_UPC.`sku_name`, _ASIN_UPC.`unit_in_listing`, _ASIN_UPC.`asin_bp_enforceable` FROM _Join_with_all_scraped_storefronts LEFT JOIN _ASIN_UPC ON _Join_with_all_scraped_storefronts.`asin` = _ASIN_UPC.`asin` AND _Join_with_all_scraped_storefronts.`country_id` = _ASIN_UPC.`country_id`
),

_Removed_unnecessary_columns AS (
  SELECT `scraped_date`, `amazonID`, `sellerId`, `sellerName`, `storefront_name`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `country` FROM _Join_Data
),

_Modify_amazonId_sellerId_storefront_name_and_sellerName AS (
  SELECT `scraped_date`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `country`, (coalesce(`amazonID`,`sellerId`)) AS `amazonID`, (coalesce(`sellerId`,`amazonID`)) AS `sellerId`, (trim(`storefront_name`)) AS `storefront_name`, (trim(`sellerName`)) AS `sellerName` FROM _Removed_unnecessary_columns
),

_Remove_Duplicated_ASINs AS (
  SELECT DISTINCT * FROM _Modify_amazonId_sellerId_storefront_name_and_sellerName
),

_change_columns_if_they_are_null AS (
  SELECT `scraped_date`, `amazonID`, `sellerId`, `storefront_name`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `country`, (CASE WHEN `offer_price` IS NULL THEN `offer_date_max_price` ELSE `offer_price` END ) AS `offer_price`, (CASE WHEN `created_date` IS NULL THEN `scraped_date` ELSE `created_date` END ) AS `created_date`, (CASE WHEN `sellerName` IS NULL THEN `storefront_name` ELSE `sellerName`END) AS `sellerName` FROM _Remove_Duplicated_ASINs
),

_Join_Gated_Status AS (
  SELECT _change_columns_if_they_are_null.`scraped_date`, _change_columns_if_they_are_null.`amazonID`, _change_columns_if_they_are_null.`sellerId`, _change_columns_if_they_are_null.`sellerName`, _change_columns_if_they_are_null.`storefront_name`, _change_columns_if_they_are_null.`asin`, _change_columns_if_they_are_null.`isQuantityRestricted`, _change_columns_if_they_are_null.`Brand`, _change_columns_if_they_are_null.`isFBA`, _change_columns_if_they_are_null.`upc`, _change_columns_if_they_are_null.`upc_name`, _change_columns_if_they_are_null.`msrp`, _change_columns_if_they_are_null.`srp`, _change_columns_if_they_are_null.`map`, _change_columns_if_they_are_null.`parent_category`, _change_columns_if_they_are_null.`child_category`, _change_columns_if_they_are_null.`child_category_2`, _change_columns_if_they_are_null.`child_category_3`, _change_columns_if_they_are_null.`partner name`, _change_columns_if_they_are_null.`country_id`, _change_columns_if_they_are_null.`offer_price`, _change_columns_if_they_are_null.`offer_date_max_price`, _change_columns_if_they_are_null.`Units`, _change_columns_if_they_are_null.`Limit Buster Units`, _change_columns_if_they_are_null.`redirect_asin`, _change_columns_if_they_are_null.`created_date`, _change_columns_if_they_are_null.`created_time`, _change_columns_if_they_are_null.`sku_map`, _change_columns_if_they_are_null.`sku_name`, _change_columns_if_they_are_null.`buy_box_count`, _change_columns_if_they_are_null.`Partner - Brand`, _change_columns_if_they_are_null.`cod_condition`, _change_columns_if_they_are_null.`unit_in_listing`, _change_columns_if_they_are_null.`bp_brand_id`, _change_columns_if_they_are_null.`asin_bp_enforceable`, _change_columns_if_they_are_null.`country`, _Select_Gated_Columns.`BBC_ORIGINAL_STATUS`, _Select_Gated_Columns.`BBC_2_8_22_STATUS` FROM _change_columns_if_they_are_null LEFT JOIN _Select_Gated_Columns ON _change_columns_if_they_are_null.`asin` = _Select_Gated_Columns.`ASIN`
),

_Join_Data_1 AS (
  SELECT _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`sales_country_id`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`revenue`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`asins units`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`revenue brand`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`asins units brand`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`Percent Mix of Brand`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`Revenue Rank R3MTH`, _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`Revenue Distribution R3MTH`, _Join_Gated_Status.`ASIN`, _Join_Gated_Status.`BBC_ORIGINAL_STATUS`, _Join_Gated_Status.`BBC_2_8_22_STATUS`, _Join_Gated_Status.`scraped_date`, _Join_Gated_Status.`amazonID`, _Join_Gated_Status.`sellerId`, _Join_Gated_Status.`sellerName`, _Join_Gated_Status.`storefront_name`, _Join_Gated_Status.`asin`, _Join_Gated_Status.`isQuantityRestricted`, _Join_Gated_Status.`Brand`, _Join_Gated_Status.`isFBA`, _Join_Gated_Status.`upc`, _Join_Gated_Status.`upc_name`, _Join_Gated_Status.`msrp`, _Join_Gated_Status.`srp`, _Join_Gated_Status.`map`, _Join_Gated_Status.`parent_category`, _Join_Gated_Status.`child_category`, _Join_Gated_Status.`child_category_2`, _Join_Gated_Status.`child_category_3`, _Join_Gated_Status.`partner name`, _Join_Gated_Status.`country_id`, _Join_Gated_Status.`offer_price`, _Join_Gated_Status.`offer_date_max_price`, _Join_Gated_Status.`Units`, _Join_Gated_Status.`Limit Buster Units`, _Join_Gated_Status.`redirect_asin`, _Join_Gated_Status.`created_date`, _Join_Gated_Status.`created_time`, _Join_Gated_Status.`sku_map`, _Join_Gated_Status.`sku_name`, _Join_Gated_Status.`buy_box_count`, _Join_Gated_Status.`Partner - Brand`, _Join_Gated_Status.`cod_condition`, _Join_Gated_Status.`unit_in_listing`, _Join_Gated_Status.`bp_brand_id`, _Join_Gated_Status.`asin_bp_enforceable`, _Join_Gated_Status.`country` FROM _ASIN_Rolling_3_Month_Revenue_Rank_DPDS RIGHT JOIN _Join_Gated_Status ON _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`asin` = _Join_Gated_Status.`asin` AND _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.`sales_country_id` = _Join_Gated_Status.`country_id`
),

_Value_Mapper AS (
  SELECT `sales_country_id`, `revenue`, `asins units`, `revenue brand`, `asins units brand`, `Percent Mix of Brand`, CASE WHEN `Revenue Rank R3MTH` IS NULL THEN '-1' ELSE `Revenue Rank R3MTH` END AS `Revenue Rank R3MTH`, `Revenue Distribution R3MTH`, `ASIN`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS`, `scraped_date`, `amazonID`, `sellerId`, `sellerName`, `storefront_name`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `country` FROM _Join_Data_1
),

_Filter_Yesterday AS (
  SELECT * FROM _Value_Mapper WHERE `scraped_date`=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
),

_Full_Limit_Buster_Inventory_DS AS (
  SELECT * FROM _Filter_Yesterday
),

_Full_Limit_Buster_Inventory_DS_Yesterday AS (
  SELECT * FROM _Filter_Yesterday
)

SELECT * FROM _Full_Limit_Buster_Inventory_DS_Yesterday