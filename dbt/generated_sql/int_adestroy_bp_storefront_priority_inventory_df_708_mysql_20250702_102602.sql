/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 708
Dataflow Name: (aDESTROY) BP Storefront Priority Inventory DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 7
  Successful: 7
  Failed: 0
  Unique Action Types: 6
  Action Types: DateCalculator, Filter, LoadFromVault, MergeJoin, PublishToVault, SelectValues
  Pipelines: 2

Generated: 2025-07-02 10:26:03
================================================================================
*/

WITH _Limit_Buster_Inventory_DS AS (
  SELECT `sales_country_id`, `revenue`, `asins units`, `revenue brand`, `asins units brand`, `Percent Mix of Brand`, `Revenue Rank R3MTH`, `Revenue Distribution R3MTH`, `scraped_date`, `amazonID`, `sellerId`, `sellerName`, `storefront_name`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `asin_active`, `asin_offers_scraper_enabled`, `asin_hero_configuration`, `asin_transparency_status`, `Country`, `sales_channel`, `Date`, `Day of Week`, `Month`, `445 Week`, `445 Week Number`, `445 Month`, `445 Month Number`, `445 Calendar`, `445 Year`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS`, `seller_id`, `storefront_enforceable`, `storefront_enforceable_code` FROM Limit_Buster_Inventory_DS
),

_BP_Storefront_Priority AS (
  SELECT * FROM BP_Storefront_Priority
),

_Calculate_Date_Difference AS (
  SELECT DATEDIFF(NULL, `scraped_date`) AS `Days Ago`, `sales_country_id`, `revenue`, `asins units`, `revenue brand`, `asins units brand`, `Percent Mix of Brand`, `Revenue Rank R3MTH`, `Revenue Distribution R3MTH`, `scraped_date`, `amazonID`, `sellerId`, `sellerName`, `storefront_name`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `asin_active`, `asin_offers_scraper_enabled`, `asin_hero_configuration`, `asin_transparency_status`, `Country`, `sales_channel`, `Date`, `Day of Week`, `Month`, `445 Week`, `445 Week Number`, `445 Month`, `445 Month Number`, `445 Calendar`, `445 Year`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS`, `seller_id`, `storefront_enforceable`, `storefront_enforceable_code` FROM _Limit_Buster_Inventory_DS
),

_Filter_Last_30_Days AS (
  SELECT * FROM _Calculate_Date_Difference WHERE `Days Ago` >= '-30'
),

_Select_Columns AS (
  SELECT `sellerId`, `sellerName`, `scraped_date`, `Units`, `revenue`, `storefront_name`, `sales_country_id`, `asins units`, `revenue brand`, `asins units brand`, `Percent Mix of Brand`, `Revenue Rank R3MTH`, `Revenue Distribution R3MTH`, `amazonID`, `asin`, `redirect_asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Limit Buster Units`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `Date`, `Day of Week`, `Month`, `445 Week`, `445 Week Number`, `445 Month`, `445 Month Number`, `445 Calendar`, `445 Year`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS`, `Days Ago` FROM _Filter_Last_30_Days
),

_Join_Data AS (
  SELECT `_BP_Storefront_Priority.*`, _Select_Columns.`sellerId`, _Select_Columns.`sellerName`, _Select_Columns.`scraped_date`, _Select_Columns.`Units`, _Select_Columns.`revenue`, _Select_Columns.`storefront_name`, _Select_Columns.`sales_country_id`, _Select_Columns.`asins units`, _Select_Columns.`revenue brand`, _Select_Columns.`asins units brand`, _Select_Columns.`Percent Mix of Brand`, _Select_Columns.`Revenue Rank R3MTH`, _Select_Columns.`Revenue Distribution R3MTH`, _Select_Columns.`amazonID`, _Select_Columns.`asin`, _Select_Columns.`redirect_asin`, _Select_Columns.`isQuantityRestricted`, _Select_Columns.`Brand`, _Select_Columns.`isFBA`, _Select_Columns.`upc`, _Select_Columns.`upc_name`, _Select_Columns.`msrp`, _Select_Columns.`srp`, _Select_Columns.`map`, _Select_Columns.`parent_category`, _Select_Columns.`child_category`, _Select_Columns.`child_category_2`, _Select_Columns.`child_category_3`, _Select_Columns.`partner name`, _Select_Columns.`country_id`, _Select_Columns.`offer_price`, _Select_Columns.`offer_date_max_price`, _Select_Columns.`Limit Buster Units`, _Select_Columns.`created_date`, _Select_Columns.`created_time`, _Select_Columns.`sku_map`, _Select_Columns.`sku_name`, _Select_Columns.`buy_box_count`, _Select_Columns.`Partner - Brand`, _Select_Columns.`cod_condition`, _Select_Columns.`unit_in_listing`, _Select_Columns.`Date`, _Select_Columns.`Day of Week`, _Select_Columns.`Month`, _Select_Columns.`445 Week`, _Select_Columns.`445 Week Number`, _Select_Columns.`445 Month`, _Select_Columns.`445 Month Number`, _Select_Columns.`445 Calendar`, _Select_Columns.`445 Year`, _Select_Columns.`BBC_ORIGINAL_STATUS`, _Select_Columns.`BBC_2_8_22_STATUS`, _Select_Columns.`Days Ago` FROM _BP_Storefront_Priority FULL JOIN _Select_Columns ON _BP_Storefront_Priority.`Seller ID` = _Select_Columns.`sellerId`
),

_aDESTROY_BP_Storefront_Priority_Inventory_DS AS (
  SELECT * FROM _Join_Data
)

SELECT * FROM _aDESTROY_BP_Storefront_Priority_Inventory_DS