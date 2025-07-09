/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 588
Dataflow Name: Radar Data ETL
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 74
  Successful: 74
  Failed: 0
  Unique Action Types: 8
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, PublishToVault, SelectValues, UnionAll
  Pipelines: 7

Generated: 2025-07-02 10:24:40
================================================================================
*/

WITH _Daily_Sales_Traffic_Data_DS AS (
  SELECT `brand_id`, `client_id`, `cogs`, `country_id`, `purchase_date`, `expanded_units`, `id`, `product_id`, `sales`, `units`, `brand_name`, `client_name`, `amazon_seller_id`, `country_1`, `Country_name`, `country_short_name`, `country_sales_channel`, `country_currency`, `Currency Value`, `Currency Symbol`, `item_price_local`, `brand name`, `partner name`, `asin`, `sku`, `upc`, `upc_name`, `upc_reorder_status`, `units_in_listing`, `currency`, `parent_category`, `child_category`, `child_category_3`, `child_category_2`, `is_bundle`, `asin_name`, `parent_asin`, `Pack Size`, `srp`, `msrp`, `brand_created_at`, `brand_start_date`, `seller_start_date`, `seller_created_at`, `account_name`, `Date`, `ad_impressions`, `ad_clicks`, `ad_units`, `ad_orders`, `ad_spend`, `ad_revenue`, `to_join_asin_performance`, `no_inventory_days`, `FBA Fulfillable DOH`, `Inbound at Amazon DOH`, `Inbound in Transit DOH`, `RC DOH`, `On Order DOH`, `buy_box_percentage`, `Weighted Average BBP`, `traff_page_views`, `traff_sessions`, `cvr`, `traff_page_views_browser`, `traff_page_views_mobile`, `traff_sessions_browser`, `traff_sessions_mobile`, `traff_units_ordered`, `traff_units_ordered_b2b`, `traff_units_ordered_total`, `dm_current_date`, `dm_relative_date`, `dm_yesterday_flag`, `dm_sdlw_flag`, `dm_sdow_flag`, `dm_last_07_flag`, `dm_prior_07_flag`, `dm_last_28_flag`, `dm_year`, `dm_ym`, `dm_month`, `dm_month_full_name`, `dm_month_short_name`, `dm_today_flag`, `dm_week_sun`, `dm_week_mon`, `dm_yr_wk`, `dm_pr_yr_wk`, `dm_prior_day_flag`, `dm_current_month_flag`, `dm_last_month_flag`, `dm_prior_month_flag`, `dm_last_year_month_flag`, `dm_prior_year_month_flag`, `dm_ytd_flag`, `dm_last_ytd_flag`, `dm_mtd_flag`, `dm_last_mtd_flag`, `dm_last_year_mtd_flag`, `dm_domo_current_date`, `dm_domo_current_time`, `dm_domo_current_timestamp`, `dm_month_int`, `dm_current_year_flag`, `dm_last_year_flag`, `dm_last_12m_flag`, `dm_prior_12m_flag`, `dm_day_of_year`, `dm_yesterday_date`, `dm_priorday_date`, `dm_ym_last`, `test_alvaro`, `dm_current_week_flag`, `current_month _flag`, `dm_last_week_flag`, `dm_prior_week_flag`, `dm_last_week_last_year_flag`, `sales_yesterday`, `sales_prior_day`, `sales_sdlw`, `sales_ytd`, `sales_mtd`, `sales_last_month_mtd`, `sales_last_year_mtd`, `sales_last_year_ytd`, `sales_current_year`, `sales_last_year`, `sales_prior_month`, `sales_last_week`, `sales_prior_week`, `sales_current_week`, `sales_last_week_last_year`, `sales_last_month`, `units_yesterday`, `units_prior_day`, `units_sdlw`, `units_ytd`, `units_mtd`, `units_last_month_mtd`, `units_last_year_mtd`, `units_least_year_ytd`, `units_month`, `units_prior_month`, `units_last_week`, `units_prior_week`, `units_last_week_last_year`, `units_last_month`, `bb_yesterday`, `bb_prior_day`, `bb_sdlw`, `bb_ytd`, `bb_mtd`, `bb_last_month_mtd`, `bb_last_year_mtd`, `bb_last_year_ytd`, `bb_current_year`, `bb_last_year`, `bb_prior_month`, `bb_last_week`, `bb_prior_week`, `bb_last_week_last_year`, `bb_last_month`, `cvr_yesterday`, `cvr_prior_day`, `cvr_sdlw`, `cvr_ytd`, `cvr_mtd`, `cvr_last_month_mtd`, `cvr_last_year_mtd`, `cvr_least_year_ytd`, `cvr_month`, `cvr_last_month`, `cvr_last_week`, `cvr_prior_week`, `cvr_last_week_last_year`, `cvr_prior_month`, `sessions_yesterday`, `sessions_prior_day`, `sessions_sdlw`, `sessions_ytd`, `sessions_mtd`, `sessions_last_month_mtd`, `sessions_last_year_mtd`, `sessions_least_year_ytd`, `sessions_month`, `sessions_last_month`, `sessions_last_week`, `sessions_prior_week`, `sessions_last_week_last_year`, `sessions_prior_month`, `ad_spend_yesterday`, `ad_spend_prior_day`, `ad_spend_sdlw`, `ad_spend_ytd`, `ad_spend_mtd`, `ad_spend_last_month_mtd`, `ad_spend_last_year_mtd`, `ad_spend_last_year_ytd`, `ad_spend_current_year`, `ad_spend_last_year`, `ad_spend_prior_month`, `ad_spend_last_week`, `ad_spend_prior_week`, `ad_spend_current_week`, `ad_spend_last_week_last_year`, `ad_spend_last_month`, `ad_sales_yesterday`, `ad_sales_prior_day`, `ad_sales_sdlw`, `ad_sales_ytd`, `ad_sales_mtd`, `ad_sales_last_month_mtd`, `ad_sales_last_year_mtd`, `ad_sales_last_year_ytd`, `ad_sales_current_year`, `ad_sales_last_year`, `ad_sales_prior_month`, `ad_sales_last_week`, `ad_sales_prior_week`, `ad_sales_current_week`, `ad_sales_last_week_last_year`, `ad_sales_last_month`, `no_inv_days_yesterday`, `no_inv_days_prior_day`, `no_inv_days_sdlw`, `no_inv_days_ytd`, `no_inv_days_mtd`, `no_inv_days_last_month_mtd`, `no_inv_days_last_year_mtd`, `no_inv_days_last_year_ytd`, `no_inv_days_current_year`, `no_inv_days_last_year`, `no_inv_days_prior_month`, `no_inv_days_last_week`, `no_inv_days_prior_week`, `no_inv_days_current_week`, `no_inv_days_last_week_last_year`, `no_inv_days_last_month`, `ad_clicks_yesterday`, `ad_clicks_prior_day`, `ad_clicks_sdlw`, `ad_clicks_ytd`, `ad_clicks_mtd`, `ad_clicks_last_month_mtd`, `ad_clicks_last_year_mtd`, `ad_clicks_last_year_ytd`, `ad_clicks_current_year`, `ad_clicks_last_year`, `ad_clicks_prior_month`, `ad_clicks_last_week`, `ad_clicks_prior_week`, `ad_clicks_current_week`, `ad_clicks_last_week_last_year`, `ad_clicks_last_month`, `ad_orders_yesterday`, `ad_orders_prior_day`, `ad_orders_sdlw`, `ad_orders_ytd`, `ad_orders_mtd`, `ad_orders_last_month_mtd`, `ad_orders_last_year_mtd`, `ad_orders_last_year_ytd`, `ad_orders_current_year`, `ad_orders_last_year`, `ad_orders_prior_month`, `ad_orders_last_week`, `ad_orders_prior_week`, `ad_orders_current_week`, `ad_orders_last_week_last_year`, `ad_orders_last_month`, `Current BSR`, `Current Customer Rating`, `Amazon Parent Category`, `Amazon Child Category 01`, `Amazon Child Category 02`, `Amazon Child Category 03`, `Amazon Child Category 04`, `Report Date`, `last_month_sales_sum`, `last_month_units_sum`, `ytd_sales_sum`, `ytd_units_sum`, `mtd_sales_sum`, `mtd_units_sum`, `last_mtd_sales_sum`, `last_mtd_units_sum`, `last_ytd_sales_sum`, `last_ytd_units_sum`, `prior_month_sales_sum`, `prior_month_units_sum`, `prior_mtd_sales_sum`, `prior_mtd_units_sum`, `last_month_sales_rank`, `last_month_units_rank`, `ytd_sales_rank`, `ytd_units_rank`, `mtd_sales_rank`, `mtd_units_rank`, `last_mtd_sales_rank`, `last_mtd_units_rank`, `last_ytd_sales_rank`, `last_ytd_units_rank`, `prior_month_sales_rank`, `prior_month_units_rank`, `prior_mtd_sales_rank`, `prior_mtd_units_rank`, `ly_cm_sales_rank`, `ly_cm_units_rank`, `brand_ytd_sales_%`, `portfolio_short_name`, `portfolio_long_name`, `product_name`, `Partner`, `Brand`, `Revenue Flag`, `sales_rank_3mth`, `% suppressed by day`, `scraper_date` FROM Daily_Sales_Traffic_Data_DS
),

_Inventory_Velocity_Daily_ASIN_Trend_DS AS (
  SELECT `asin`, `amazon_seller_id`, `country_id`, `date_of_first_sale`, `purchase_date`, `amazon_inventory_log_country`, `afn_fulfillable_quantity`, `reserved_fc_transfers`, `reserved_fc_processing`, `inbound_RECEIVING`, `inbound_CHECKED_IN`, `inbound_DELIVERED`, `inbound_IN_TRANSIT`, `inbound_WORKING`, `inbound_SHIPPED`, `PO_CREATED`, `PO_APPROVED`, `PO_INVOICED`, `PO_RECEIVED`, `PO_PROCESSED`, `On Order`, `FBA_MFN_AVAILABLE`, `Local On Hand`, `asin_cnt`, `Brand`, `Partner`, `is_discontinued_status`, `map`, `msrp`, `No Inventory`, `No Inv n rf_proc`, `NoInventoryCount_28Days`, `NoInventoryCount_21Days`, `NoInventoryCount_14Days`, `NoInventoryCount_07Days`, `NoInventoryCount_56Days`, `NoInventoryCount_84Days`, `sale_date`, `units_asin_a`, `units_upc_a`, `revenue_a`, `vendor_sku`, `asin_name`, `units_asin`, `units_upc`, `revenue`, `Units_asin_2`, `L28 Revenue`, `L28 Demand`, `L28 No Inventory`, `L56 Demand`, `L56 No Inventory`, `L84 Demand`, `L84 No Inventory`, `Count 28 Days`, `Countr 56 Days`, `L7 Demand`, `L7 No Inventory`, `Full OOS Day`, `Partial OOS Day`, `L28 Normalized`, `L56 Normalized`, `L28 Normalized CNT`, `L56 Normalized CNT`, `DOW`, `L07 Actual`, `YQ`, `Relative Date`, `units_asin_stdev`, `units_upc_stdev`, `units_asin_median`, `units_upc_median`, `units_asin_avg`, `units_upc_avg`, `Avg Price 90Day`, `Max Demand`, `P90 Units`, `Min NonZero Demand`, `P20 Units`, `P80 Units`, `Calculated Days`, `P10 Units`, `P95 Units`, `P97 Units`, `ltv_units_asin_stdev`, `ltv_units_asin_avg`, `ltv_units_upc_avg`, `LTV Avg Price`, `LTV P90 Units`, `LTV P20 Units`, `LTV P80 Units`, `Back Fill Avg`, `Back Fill Type`, `Noise`, `Back Fill`, `ADS 90D`, `LT 1STDEV`, `Total Inventory`, `IAD Prep`, `IAD`, `IAD_M80`, `IAD_Classification`, `IAD Capped`, `Full Day Cost IAD`, `Partial Day Cost IAD`, `Total Cost IAD`, `is_adjusted`, `is_adjusted_capped`, `L28 CNT Days`, `L14 CNT Days`, `L07 CNT Days`, `IAD_Adjusted_Count`, `IAD_Adjusted_Cap_Count`, `L28_IAD_M80`, `L28_IAD`, `L14_IAD`, `L07_IAD`, `L07_IAD_M80`, `L28_IAD_CAP`, `L28 ILO Value`, `DateRank`, `L28_CAP_LAG07`, `L28_CAP_LAG01`, `L28_CAP_LAG28`, `IAD_R28`, `IAD_R28M`, `IAD_R14`, `IAD_R07`, `IAD_R07M`, `IAD_CAP_R28`, `IAD_CAP_R28_LAG07`, `IAD_CAP_R28_LAG01`, `IAD_CAP_R28_LAG28`, `DoD AV1 Trend`, `WoW AV1 Trend`, `MoM AV1 Trend`, `IAD 28 Is Adjusted`, `IAD 28 Is Adjusted Capped`, `child_asin`, `amazon_seller_central_id`, `aap_country_id`, `page_views_owned`, `page_view_total`, `BBP Max`, `BBP Avg`, `buy_box_percentage`, `AV1`, `AV2`, `AV3`, `DoD Change Flag`, `WoW Change Flag`, `MoM Change Flag`, `PurchaseYM`, `date`, `Rank & Window 5.amazon_seller_central_id`, `Rank & Window 5.aap_country_id`, `Year-Qtr`, `Year`, `BB_R28_AVG`, `BB_PV_OWNED_SUM_28`, `BB_PV_TOTAL_SUM_28`, `BB-Q 1.aap_country_id`, `BB-Q 1.amazon_seller_central_id`, `BB_P15Q`, `BB_P75Q`, `BB_AVGQ`, `BB_P50Q`, `BB_SDQ`, `BB-Y.aap_country_id`, `BB-Y.amazon_seller_central_id`, `BB_P15Y`, `BB_P75Y`, `BB_AVGY`, `BB_P50Y`, `BB_SDY`, `BB-ALL TIME.child_asin`, `BB-ALL TIME.aap_country_id`, `BB-ALL TIME.amazon_seller_central_id`, `BB_P15_AT`, `BB_P75_AT`, `BB_AVG_AT`, `BB_P50_AT`, `BB_SD_AT`, `OOS Risk Flag`, `AccountName`, `product_name`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `Vel Status Bins`, `Select Columns.Brand`, `partner name`, `AV1 Bins`, `DOH Bins`, `Catalog Last Appearence`, `Business Unit`, `Active_Reorder_Status_Count`, `upc_reorder`, `cost`, `inv_age_0_to_90_days`, `inv_age_91_to_180_days`, `inv_age_181_to_270_days`, `inv_age_271_to_365_days1`, `inv_age_365_plus_days`, `sku_cost`, `Select Columns.map`, `SRP`, `Currency`, `Country Name`, `Domain Sales Channel`, `Country Name Short`, `AD_ID`, `BBAD_P50Q`, `BBAD_P50Q_Valuation`, `BBAD_R28`, `BBAD_R28_Valuation`, `BBAD_P50_DMD_Units`, `BBAD_P50_EST_Units`, `BBAD_P50_NEW__BB`, `BBAD_M80_Units`, `BBAD_M80_Valuation`, `Loss Opportunity By Type`, `Loss Opportunity By Type M80`, `Inventory Fault Valuation`, `Buy Box Fault Valuation`, `Country`, `Hero`, `FBA Fulfillable DOH`, `Inbound @ Amazon DOH`, `Inbound in Transit DOH`, `RC DOH`, `On Order DOH`, `Target DoC`, `Brand Status`, `Purchaser`, `Active Purchasing`, `Hero Target DoC`, `Total Inventory DOH`, `Overstock Flag`, `Overstock Value`, `Baseline Value`, `Overstock QTY` FROM Inventory_Velocity_Daily_ASIN_Trend_DS
),

_Enforcement_DS AS (
  SELECT `upc`, `vendor_code`, `catalog_map`, `sales_country_id`, `revenue`, `asins units`, `revenue brand`, `asins units brand`, `Percent Mix of Brand`, `Revenue Rank R3MTH`, `Revenue Distribution R3MTH`, `scraped_date`, `amazonID`, `sellerId`, `sellerName`, `storefront_name`, `asin`, `isQuantityRestricted`, `Brand`, `isFBA`, `upc_name`, `msrp`, `srp`, `map`, `parent_category`, `child_category`, `child_category_2`, `child_category_3`, `partner name`, `country_id`, `offer_price`, `offer_date_max_price`, `Units`, `Limit Buster Units`, `redirect_asin`, `created_date`, `created_time`, `sku_map`, `sku_name`, `buy_box_count`, `Partner - Brand`, `cod_condition`, `unit_in_listing`, `bp_brand_id`, `asin_bp_enforceable`, `asin_active`, `asin_offers_scraper_enabled`, `asin_hero_configuration`, `asin_transparency_status`, `Country`, `sales_channel`, `Date`, `Day of Week`, `Month`, `445 Week`, `445 Week Number`, `445 Month`, `445 Month Number`, `445 Calendar`, `445 Year`, `BBC_ORIGINAL_STATUS`, `BBC_2_8_22_STATUS`, `seller_id`, `storefront_enforceable`, `storefront_enforceable_code`, `marketplace_channel_id`, `marketplace_channel`, `id`, `enforcement_configuration_id`, `brand_assigned_user`, `brand_assigned_user_initials`, `seller brands cols.storefront_enforceable`, `brand_enforceable`, `assigned_user_id`, `enforce_by`, `group_partner_brand`, `Storefront ID`, `Partner`, `Last Notice Type`, `Last Notice Sent`, `Last Note`, `Invoice ID`, `Job ID`, `USPS Tracking`, `Delivery Status`, `Delivery Date`, `Enforcement History`, `C2M ERROR_STATUS`, `Recipient Address`, `Notes`, `Enter Date`, `New Notes`, `New Invoice ID` FROM Enforcement_DS
),

_IDQ_ASIN_Performance_DS AS (
  SELECT `Select Columns 1.brand`, `asin`, `idq_score`, `date`, `title`, `review_ratings`, `review_avg_rating`, `idq_grade`, `amazon_asin_id`, `client_id`, `brand_id`, `client_name`, `Brand` FROM IDQ_ASIN_Performance_DS
),

_RADAR_COBALT_INPUT AS (
  SELECT `BRAND`, `START_DATE`, `Brand Sales`, `anchor`, `greatest_date`, `Brand Core Category`, `Category Sales`, `dm_current_date`, `dm_relative_date`, `dm_year`, `dm_ym`, `dm_month`, `dm_month_full_name`, `dm_month_short_name`, `dm_today_flag`, `dm_pr_yr_wk`, `dm_current_month_flag`, `dm_last_month_flag`, `dm_prior_month_flag`, `dm_last_year_month_flag`, `dm_prior_year_month_flag`, `dm_ytd_flag`, `dm_last_ytd_flag`, `dm_mtd_flag`, `dm_last_mtd_flag`, `dm_last_year_mtd_flag`, `dm_domo_current_date`, `dm_domo_current_time`, `dm_domo_current_timestamp`, `dm_month_int`, `dm_current_year_flag`, `dm_last_year_flag`, `dm_last_12_flag`, `dm_prior_12_flag`, `dm_last_3_month_flag`, `dm_last_P3_month_flag`, `dm_p3_max_date`, `brand_sales_ytd`, `category_sales_ytd`, `brand_sales_last_12`, `category_sales_last_12`, `brand_sales_prior_12`, `category_sales_prior_12`, `brand_sales_current_month`, `category_sales_current_month`, `brand_sales_last_month`, `category_sales_last_month`, `brand_sales_last_ytd`, `category_sales_last_ytd`, `brand_sales_last_3mth`, `category_sales_last_3mth`, `brand_sales_prior_3mth`, `category_sales_prior_3mth` FROM aDESTROY_RADAR_COBALT_INPUT
),

_Cleaned_up_Scrapped_Cat_Leaf_Ratings AS (
  SELECT `asin`, `bb_information`, `best_seller_rank`, `categories`, `created_at`, `fastest_delivery`, `features`, `id`, `images`, `important_information`, `is_404`, `is_out_of_stock`, `offers`, `price`, `product_details`, `product_overview`, `rating`, `detail_rating`, `total_review_count`, `customers_say`, `review_tags`, `redirect_asin`, `return_policy`, `seller_id`, `storefront`, `title`, `updated_at`, `videos`, `country_id`, `full_text_generated`, `seller_name`, `velocity`, `sales`, `fba_inventory`, `_BATCH_ID_`, `_BATCH_LAST_RUN_`, `Cleaned-Up Category Path`, `Cleaned Up Ratings Step 1`, `Parent Node`, `Leaf Node 01`, `Leaf Node 02`, `Leaf Node 03`, `Leaf Node 04`, `Brand`, `Partner`, `SKU`, `SKU Count`, `sales_ytd`, `units_ytd` FROM Cleaned_up_Scrapped_Cat_Leaf_Ratings
),

_Advertising_Metrics_All_Orders AS (
  SELECT `ID`, `CustomerID`, `CustomerName`, `BrandName`, `Category`, `ADType`, `CompetitorTactic`, `CampaignID`, `CampaignName`, `TargetingType`, `AdGroupID`, `AdGroupName`, `KeywordID`, `KeywordText`, `MatchType`, `Impressions`, `Clicks`, `Cost`, `CPC`, `CTR`, `Revenue`, `ROAS`, `ACOS`, `CostPerConv`, `ConvRate`, `Orders`, `Units`, `NTBOrders`, `NTBOrdersPerc`, `NTBUnits`, `NTBUnitsPerc`, `NTBSales`, `NTBSalesPerc`, `OPC`, `BrandDayOcurrences`, `Date`, `brand_id`, `BrandDayOrdersSold`, `BrandDaySalesAmount`, `DPV`, `PortfolioID`, `PortfolioName`, `DSP Total product sales`, `DSP Total purchases`, `DSP Total units sold`, `DPVR`, `CurrencyCode`, `portfolio_client_id`, `master_portfolio_id`, `master_portfolio_name`, `master_portfolio_brand_id`, `master_portfolio_use_dsp_budget`, `TargettingType`, `Category V2`, `Targetting Level`, `TargetingMethod`, `Category - New`, `AD Type - New`, `Targeting Type - New`, `Short Name`, `Campaign Label - Tactic`, `Strategy - New`, `keyword_name`, `Advertising Tactic`, `table`, `Month`, `Year`, `OcurrenciesByMonth`, `DateYM`, `Calculated NTBOrders`, `Calculated NTBSales`, `Brand`, `CountOrders`, `CountBuyerEmail`, `FinalOPC`, `Group By 1.Brand`, `purchase_date`, `avg_opc`, `Targeting Type` FROM Advertising_Metrics_All_Orders
),

_Filter_Rows AS (
  SELECT * FROM _Daily_Sales_Traffic_Data_DS WHERE `dm_relative_date` >= '365'
),

_Last_28_D_1 AS (
  SELECT * FROM _Daily_Sales_Traffic_Data_DS WHERE `dm_relative_date` >= '28'
),

_Group_By_11 AS (
  SELECT `brand name`, SUM(`sales`) AS `brand_sales`, SUM(`brand_sales_last_12m`) AS `brand_sales_last_12m`, SUM(`brand_sales_prior_12m`) AS `brand_sales_prior_12m`, SUM(`sales_ytd`) AS `brand_sales_cy_ytd`, SUM(`sales_last_year_ytd`) AS `brand_sales_py_ytd` FROM _Daily_Sales_Traffic_Data_DS GROUP BY `brand name`
),

_Last_28_D AS (
  SELECT * FROM _Inventory_Velocity_Daily_ASIN_Trend_DS WHERE `Relative Date` >= '-28'
),

_Filter_Rows_3 AS (
  SELECT * FROM _Enforcement_DS WHERE DATE(`scraped_date`) = date(DATE_SUB(DATE_SUB(CURRENT_TIMESTAMP(),INTERVAL 8 hour),interval 3 day))
),

_Group_By_4 AS (
  SELECT `asin`, MAX(`date`) AS `last_run_date` FROM _IDQ_ASIN_Performance_DS GROUP BY `asin`
),

_Group_By_8 AS (
  SELECT `BRAND`, SUM(`brand_sales_ytd`) AS `cobalt_brand_sales_ytd`, SUM(`category_sales_ytd`) AS `category_sales_ytd`, SUM(`brand_sales_last_12`) AS `cobalt_brand_sales_last_12`, SUM(`category_sales_last_12`) AS `category_sales_last_12`, SUM(`brand_sales_prior_12`) AS `cobalt_brand_sales_prior_12`, SUM(`category_sales_prior_12`) AS `category_sales_prior_12`, SUM(`cobalt_brand_l12_p12`) AS `cobalt_brand_l12_p12`, SUM(`category_l12_p12`) AS `category_l12_p12`, SUM(`cobalt_brand_mom`) AS `cobalt_brand_mom`, SUM(`category_mom`) AS `category_mom`, SUM(`cobalt_Index12M`) AS `cobalt_Index12M`, SUM(`cobalt_IndexM`) AS `cobalt_IndexM`, SUM(`category_lytd_pytd`) AS `category_lytd_pytd`, SUM(`cobalt_brand_lytd_pytd`) AS `cobalt_brand_lytd_pytd` FROM _RADAR_COBALT_INPUT GROUP BY `BRAND`
),

_Filter_Rows_2 AS (
  SELECT * FROM _Advertising_Metrics_All_Orders WHERE `Date` >= DATE_SUB(CURRENT_DATE(),INTERVAL 28 day)
),

_Group_By AS (
  SELECT `country_id`, `asin`, `brand name`, SUM(`sales`) AS `sales_last 365_days_asin` FROM _Filter_Rows GROUP BY `country_id`, `asin`, `brand name`
),

_Group_By_2 AS (
  SELECT `country_id`, `brand name`, SUM(`sales`) AS `sales_last 365_days_brand` FROM _Filter_Rows GROUP BY `country_id`, `brand name`
),

_Buy_Box_Branch AS (
  SELECT `country_id`, `brand name`, `asin`, COUNT(DISTINCT `purchase_date`) AS `Number of Unique Days`, AVG(`buy_box_percentage`) AS `Buy Box`, SUM(`sales`) AS `sales_asin_bb` FROM _Last_28_D_1 GROUP BY `country_id`, `brand name`, `asin`
),

_Buy_Box_Branch_1 AS (
  SELECT `country_id`, `brand name`, SUM(`sales`) AS `sales_brand_bb` FROM _Last_28_D_1 GROUP BY `country_id`, `brand name`
),

_Group_By_1 AS (
  SELECT `country_id`, `Brand`, `asin`, SUM(`No Inventory`) AS `No Inventory Sum`, AVG(`No Inventory`) AS `No Inventory Avg`, SUM(`In-Stock Rate Avg`) AS `In-Stock Rate Avg`, COUNT(DISTINCT `purchase_date`) AS `Number of Unique Days` FROM _Last_28_D GROUP BY `country_id`, `Brand`, `asin`
),

_Group_By_9 AS (
  SELECT `country_id`, `Brand`, `asin`, COUNT(DISTINCT `seller_id`) AS `Number of Sellers` FROM _Filter_Rows_3 GROUP BY `country_id`, `Brand`, `asin`
),

_Join_Data_2 AS (
  SELECT _IDQ_ASIN_Performance_DS.`Select Columns 1.brand`, _IDQ_ASIN_Performance_DS.`asin`, _IDQ_ASIN_Performance_DS.`idq_score`, _IDQ_ASIN_Performance_DS.`date`, _IDQ_ASIN_Performance_DS.`title`, _IDQ_ASIN_Performance_DS.`review_ratings`, _IDQ_ASIN_Performance_DS.`review_avg_rating`, _IDQ_ASIN_Performance_DS.`idq_grade`, _IDQ_ASIN_Performance_DS.`amazon_asin_id`, _IDQ_ASIN_Performance_DS.`client_id`, _IDQ_ASIN_Performance_DS.`brand_id`, _IDQ_ASIN_Performance_DS.`client_name`, _IDQ_ASIN_Performance_DS.`Brand`, _Group_By_4.`last_run_date` FROM _IDQ_ASIN_Performance_DS LEFT JOIN _Group_By_4 ON _IDQ_ASIN_Performance_DS.`asin` = _Group_By_4.`asin`
),

_Join_Data_6 AS (
  SELECT _Group_By_11.`brand name`, _Group_By_11.`brand_sales`, _Group_By_11.`brand_sales_last_12m`, _Group_By_11.`brand_sales_prior_12m`, _Group_By_11.`brand_sales_cy_ytd`, _Group_By_11.`brand_sales_py_ytd`, _Group_By_8.`BRAND` AS `cobalt_brand`, _Group_By_8.`cobalt_brand_sales_ytd`, _Group_By_8.`category_sales_ytd`, _Group_By_8.`cobalt_brand_sales_last_12`, _Group_By_8.`category_sales_last_12`, _Group_By_8.`cobalt_brand_sales_prior_12`, _Group_By_8.`category_sales_prior_12`, _Group_By_8.`cobalt_brand_l12_p12`, _Group_By_8.`category_l12_p12`, _Group_By_8.`cobalt_brand_mom`, _Group_By_8.`category_mom`, _Group_By_8.`cobalt_Index12M`, _Group_By_8.`cobalt_IndexM`, _Group_By_8.`category_lytd_pytd`, _Group_By_8.`cobalt_brand_lytd_pytd` FROM _Group_By_11 LEFT JOIN _Group_By_8 ON _Group_By_11.`brand name` = _Group_By_8.`BRAND`
),

_Group_By_5 AS (
  SELECT `BrandName`, SUM(`Cost`) AS `Ad Spend`, SUM(`Revenue`) AS `Ad Revenue`, SUM(`ACOS`) AS `ACOS`, SUM(`Ad ASP`) AS `Ad ASP`, SUM(`ROAS`) AS `ROAS`, SUM(`ROAS Per ASP`) AS `ROAS Per ASP` FROM _Filter_Rows_2 GROUP BY `BrandName`
),

_Sales_365 AS (
  SELECT _Group_By.`country_id`, _Group_By.`asin`, _Group_By.`brand name`, _Group_By.`sales_last 365_days_asin`, _Group_By_2.`sales_last 365_days_brand` FROM _Group_By LEFT JOIN _Group_By_2 ON _Group_By.`brand name` = _Group_By_2.`brand name` AND _Group_By.`country_id` = _Group_By_2.`country_id`
),

_Join_Data_3 AS (
  SELECT _Buy_Box_Branch.`country_id`, _Buy_Box_Branch.`brand name`, _Buy_Box_Branch.`asin`, _Buy_Box_Branch.`Number of Unique Days`, _Buy_Box_Branch.`Buy Box`, _Buy_Box_Branch.`sales_asin_bb`, _Buy_Box_Branch_1.`sales_brand_bb` FROM _Buy_Box_Branch LEFT JOIN _Buy_Box_Branch_1 ON _Buy_Box_Branch.`brand name` = _Buy_Box_Branch_1.`brand name` AND _Buy_Box_Branch.`country_id` = _Buy_Box_Branch_1.`country_id`
),

_latest_data AS (
  SELECT * FROM _Join_Data_2 WHERE `date` >= `last_run_date`
),

_Add_Formula_5 AS (
  SELECT `brand name`, `brand_sales`, `brand_sales_last_12m`, `brand_sales_prior_12m`, `brand_sales_cy_ytd`, `brand_sales_py_ytd`, `cobalt_brand`, `cobalt_brand_sales_ytd`, `category_sales_ytd`, `cobalt_brand_sales_last_12`, `category_sales_last_12`, `cobalt_brand_sales_prior_12`, `category_sales_prior_12`, `cobalt_brand_l12_p12`, `category_l12_p12`, `cobalt_brand_mom`, `category_mom`, `cobalt_Index12M`, `cobalt_IndexM`, `category_lytd_pytd`, `cobalt_brand_lytd_pytd`, ( `brand_sales_last_12m` / `brand_sales_prior_12m`) AS `brand_l12_p12`, (`brand_sales_cy_ytd` / `brand_sales_py_ytd`) AS `brand_lytd_pytd`, (case when `cobalt_brand` is NULL then 1 else 0 END) AS `no_cobalt_match_flag`, (`cobalt_brand_l12_p12` / `cobalt_brand_sales_prior_12`) AS `cobalt_brand_l12_p12_`, (`brand_l12_p12` / `category_l12_p12`) AS `brand_category_Index_12m`, (`brand_lytd_pytd` / `category_lytd_pytd`) AS `brand_category_index_ytd`, (`cobalt_brand_l12_p12_` / `category_l12_p12`) AS `cobalt_brand_cat_index_12` FROM _Join_Data_6
),

_Add_Formula_3 AS (
  SELECT `BrandName`, `Ad Spend`, `Ad Revenue`, `ACOS`, `Ad ASP`, `ROAS`, `ROAS Per ASP`, (round((1-`ACOS`) * 100,0)) AS `Metric Value`, ('ACOS') AS `Metric`, ('Ads') AS `Element`, (1) AS `country_id` FROM _Group_By_5
),

_Join_Data AS (
  SELECT _Group_By_1.`country_id`, _Group_By_1.`Brand`, _Group_By_1.`asin`, _Group_By_1.`No Inventory Sum`, _Group_By_1.`No Inventory Avg`, _Group_By_1.`In-Stock Rate Avg`, _Group_By_1.`Number of Unique Days`, _Sales_365.`brand name`, _Sales_365.`sales_last 365_days_asin`, _Sales_365.`sales_last 365_days_brand` FROM _Group_By_1 LEFT JOIN _Sales_365 ON _Group_By_1.`Brand` = _Sales_365.`brand name` AND _Group_By_1.`country_id` = _Sales_365.`country_id` AND _Group_By_1.`asin` = _Sales_365.`asin`
),

_Join_Data_11 AS (
  SELECT _Join_Data_3.`country_id`, _Join_Data_3.`brand name`, _Join_Data_3.`asin`, _Join_Data_3.`Number of Unique Days`, _Join_Data_3.`Buy Box`, _Join_Data_3.`sales_asin_bb`, _Join_Data_3.`sales_brand_bb`, _Group_By_9.`Number of Sellers` FROM _Join_Data_3 LEFT JOIN _Group_By_9 ON _Join_Data_3.`asin` = _Group_By_9.`asin` AND _Join_Data_3.`brand name` = _Group_By_9.`Brand` AND _Join_Data_3.`country_id` = _Group_By_9.`country_id`
),

_Filter_Rows_1 AS (
  SELECT * FROM _Join_Data_3 WHERE `country_id` = '1'
),

_Add_Formula_2 AS (
  SELECT `Select Columns 1.brand`, `asin`, `idq_score`, `date`, `title`, `review_ratings`, `review_avg_rating`, `idq_grade`, `amazon_asin_id`, `client_id`, `brand_id`, `client_name`, `Brand`, `last_run_date`, (case when `idq_score` < 1 then `idq_score` * 100 else `idq_score` END) AS `IDQ Normalized` FROM _latest_data
),

_Add_Formula_6 AS (
  SELECT `brand name`, `brand_sales`, `brand_sales_last_12m`, `brand_sales_prior_12m`, `brand_sales_cy_ytd`, `brand_sales_py_ytd`, `cobalt_brand`, `cobalt_brand_sales_ytd`, `category_sales_ytd`, `cobalt_brand_sales_last_12`, `category_sales_last_12`, `cobalt_brand_sales_prior_12`, `category_sales_prior_12`, `cobalt_brand_l12_p12`, `category_l12_p12`, `cobalt_brand_mom`, `category_mom`, `cobalt_Index12M`, `cobalt_IndexM`, `category_lytd_pytd`, `cobalt_brand_lytd_pytd`, `brand_l12_p12`, `brand_lytd_pytd`, `brand_category_Index_12m`, `brand_category_index_ytd`, `no_cobalt_match_flag`, `cobalt_brand_l12_p12_`, `cobalt_brand_cat_index_12`, ('Momentum') AS `Element`, (case when `brand_category_index_ytd` > 1 and `brand_category_Index_12m` > 1 then 100 when `brand_category_index_ytd` > 0.75 and `brand_category_Index_12m` > 0.75 then 75 when `brand_category_index_ytd` > 1 and `brand_category_Index_12m` < 1 then 65 when `brand_category_index_ytd` < 1 and `brand_category_Index_12m` > 1 then 50 when `brand_category_index_ytd` > 0.5 and `brand_category_Index_12m` > 0.5 then 50 when `brand_category_index_ytd` < 0.5 and `brand_category_Index_12m` < 0.5 then 25 when `brand_category_index_ytd` < 0.5 then 25 else 55 END ) AS `Metric`, (1) AS `country_id` FROM _Add_Formula_5
),

_Select_Columns_11 AS (
  SELECT `BrandName` AS `Brand`, `Metric Value`, `Element`, `country_id` FROM _Add_Formula_3
),

_Select_Columns_15 AS (
  SELECT `BrandName` AS `Brand`, `Metric Value` AS `Ads`, `country_id` FROM _Add_Formula_3
),

_Add_Formula AS (
  SELECT `country_id`, `Brand`, `asin`, `No Inventory Sum`, `No Inventory Avg`, `In-Stock Rate Avg`, `Number of Unique Days`, `brand name`, `sales_last 365_days_asin`, `sales_last 365_days_brand`, (COALESCE(`sales_last 365_days_asin`* `In-Stock Rate Avg`,0)) AS `Sales Weight`, ('Product Availability') AS `Element`, (CASE WHEN (`Sales Weight` / `sales_last 365_days_brand`) IS NULL THEN 0 ELSE (`Sales Weight` / `sales_last 365_days_brand`) END) AS `In-Stock Weight` FROM _Join_Data
),

_Add_Formula_4 AS (
  SELECT `country_id`, `brand name`, `asin`, `Number of Unique Days`, `Buy Box`, `sales_asin_bb`, `sales_brand_bb`, `Number of Sellers`, ( (CASE when `Number of Sellers` = 1 then 100 when `Number of Sellers` <= 5 then 75 when `Number of Sellers` <= 10 then 50 when `Number of Sellers` <= 20 then 25 when `Number of Sellers` >= 20 then 0 END) ) AS `Seller Points`, (`Buy Box`) AS `Buy Box Points`, (( 0.3 * `Seller Points` ) + ( 0.7 * `Buy Box Points` )) AS `Market Control Points` FROM _Join_Data_11
),

_Join_Data_10 AS (
  SELECT _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`asin`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`bb_information`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`best_seller_rank`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`categories`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`created_at`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`fastest_delivery`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`features`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`id`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`images`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`important_information`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`is_404`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`is_out_of_stock`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`offers`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`price`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`product_details`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`product_overview`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`rating`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`detail_rating`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`total_review_count`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`customers_say`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`review_tags`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`redirect_asin`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`return_policy`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`seller_id`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`storefront`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`title`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`updated_at`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`videos`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`country_id`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`full_text_generated`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`seller_name`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`velocity`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`sales`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`fba_inventory`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`_BATCH_ID_`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`_BATCH_LAST_RUN_`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Cleaned-Up Category Path`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Cleaned Up Ratings Step 1`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Parent Node`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Leaf Node 01`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Leaf Node 02`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Leaf Node 03`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Leaf Node 04`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Brand`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Partner`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`SKU`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`SKU Count`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`sales_ytd`, _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`units_ytd`, _Filter_Rows_1.`brand name`, _Filter_Rows_1.`Number of Unique Days`, _Filter_Rows_1.`Buy Box`, _Filter_Rows_1.`sales_asin_bb`, _Filter_Rows_1.`sales_brand_bb` FROM _Cleaned_up_Scrapped_Cat_Leaf_Ratings LEFT JOIN _Filter_Rows_1 ON _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`Brand` = _Filter_Rows_1.`brand name` AND _Cleaned_up_Scrapped_Cat_Leaf_Ratings.`asin` = _Filter_Rows_1.`asin`
),

_Group_By_7 AS (
  SELECT `Brand`, `asin`, AVG(`IDQ Normalized`) AS `IDQ Score`, COUNT(DISTINCT `date`) AS `IDQ Unique Days`, AVG(`review_avg_rating`) AS `Avg Ratings`, SUM(`review_ratings`) AS `Reviewers`, SUM(`Avg Ratings 100`) AS `Avg Ratings 100`, SUM(`country_id`) AS `country_id` FROM _Add_Formula_2 GROUP BY `Brand`, `asin`
),

_Select_Columns_8 AS (
  SELECT `brand name` AS `Brand`, `Element`, `Metric` AS `Metric Value` FROM _Add_Formula_6
),

_Select_Columns_14 AS (
  SELECT `brand name` AS `Brand`, `Metric` AS `Momentum`, `country_id` FROM _Add_Formula_6
),

_Group_By_3 AS (
  SELECT `country_id`, `Brand`, `Element`, SUM(`Weighted Brand In-Stock Rate`) AS `Weighted Brand In-Stock Rate` FROM _Add_Formula GROUP BY `country_id`, `Brand`, `Element`
),

_Select_Columns AS (
  SELECT `country_id`, `asin`, `sales_last 365_days_asin` AS `Sales ASIN Weight`, `Brand`, `In-Stock Rate Avg` AS `Metric`, `Sales Weight` AS `Sales Weight`, `In-Stock Weight` AS `Metric Weight`, `Element`, `sales_last 365_days_brand` AS `Sales Brand Weight` FROM _Add_Formula
),

_Add_Formula_1 AS (
  SELECT `country_id`, `brand name`, `asin`, `Number of Unique Days`, `Buy Box`, `sales_asin_bb`, `sales_brand_bb`, `Number of Sellers`, `Seller Points`, `Buy Box Points`, `Market Control Points`, (COALESCE( `sales_asin_bb` * `Market Control Points`,0)) AS `Sales Weight`, ('Market Control') AS `Element`, (CASE WHEN (`Sales Weight` / `sales_brand_bb`) IS NULL THEN 0 ELSE (`Sales Weight` / `sales_brand_bb`) END) AS `Buy Box Weight` FROM _Add_Formula_4
),

_Ratings AS (
  SELECT `country_id`, `brand name`, `asin`, `Number of Unique Days`, `Buy Box`, `sales_asin_bb`, `sales_brand_bb`, `bb_information`, `best_seller_rank`, `categories`, `created_at`, `fastest_delivery`, `features`, `id`, `images`, `important_information`, `is_404`, `is_out_of_stock`, `offers`, `price`, `product_details`, `product_overview`, `rating`, `detail_rating`, `total_review_count`, `customers_say`, `review_tags`, `redirect_asin`, `return_policy`, `seller_id`, `storefront`, `title`, `updated_at`, `videos`, `full_text_generated`, `seller_name`, `velocity`, `sales`, `fba_inventory`, `_BATCH_ID_`, `_BATCH_LAST_RUN_`, `Cleaned-Up Category Path`, `Cleaned Up Ratings Step 1`, `Parent Node`, `Leaf Node 01`, `Leaf Node 02`, `Leaf Node 03`, `Leaf Node 04`, `Brand`, `Partner`, `SKU`, `SKU Count`, `sales_ytd`, `units_ytd`, (COALESCE(`sales_asin_bb` *  `Cleaned Up Ratings Step 1`* 20 ,0)) AS `Sales Weight`, ('Customer Satisfaction') AS `Element`, (CASE WHEN (`Sales Weight` / `sales_brand_bb`) IS NULL THEN 0 ELSE (`Sales Weight` / `sales_brand_bb`) END) AS `Ratings Weight` FROM _Join_Data_10
),

_Join_Data_1 AS (
  SELECT _Group_By_7.`Brand`, _Group_By_7.`asin`, _Group_By_7.`IDQ Score`, _Group_By_7.`IDQ Unique Days`, _Group_By_7.`Avg Ratings`, _Group_By_7.`Reviewers`, _Group_By_7.`Avg Ratings 100`, _Group_By_7.`country_id`, _Filter_Rows_1.`brand name`, _Filter_Rows_1.`Number of Unique Days`, _Filter_Rows_1.`Buy Box`, _Filter_Rows_1.`sales_asin_bb` AS `sales_asin_idq`, _Filter_Rows_1.`sales_brand_bb` AS `sales_brand_idq` FROM _Group_By_7 LEFT JOIN _Filter_Rows_1 ON _Group_By_7.`Brand` = _Filter_Rows_1.`brand name` AND _Group_By_7.`asin` = _Filter_Rows_1.`asin`
),

_IS_Append AS (
  SELECT `country_id`, `Brand`, `Weighted Brand In-Stock Rate` AS `Metric Value`, `Element` FROM _Group_By_3
),

_IS_Join AS (
  SELECT `country_id`, `Brand`, `Weighted Brand In-Stock Rate` AS `In-Stock Rate` FROM _Group_By_3
),

_Group_By_6 AS (
  SELECT `country_id`, `brand name`, `Element`, SUM(`Weighted Brand Buy Box`) AS `Weighted Brand Buy Box` FROM _Add_Formula_1 GROUP BY `country_id`, `brand name`, `Element`
),

_Select_Columns_2 AS (
  SELECT `country_id`, `asin`, `brand name` AS `Brand`, `Number of Unique Days`, `Sales Weight` AS `Sales Weight`, `Buy Box` AS `Metric Value`, `Buy Box Weight` AS `Metric Weight`, `Element`, `sales_asin_bb` AS `Sales ASIN Weight`, `sales_brand_bb` AS `Sales Brand Weight` FROM _Add_Formula_1
),

_Group_By_12 AS (
  SELECT `Brand`, `Element`, `country_id`, SUM(`Weighted Brand Avg Rating`) AS `Weighted Brand Avg Rating` FROM _Ratings GROUP BY `Brand`, `Element`, `country_id`
),

_Select_Columns_5 AS (
  SELECT `asin`, `brand name` AS `Brand`, `Sales Weight`, `Ratings Weight` AS `Metric Weight`, `sales_asin_bb` AS `Sales ASIN Weight`, `sales_brand_bb` AS `Sales Brand Weight`, `Cleaned Up Ratings Step 1` AS `Metric` FROM _Ratings
),

_IDQ AS (
  SELECT `Brand`, `asin`, `IDQ Score`, `IDQ Unique Days`, `Avg Ratings`, `Reviewers`, `Avg Ratings 100`, `country_id`, `brand name`, `Number of Unique Days`, `Buy Box`, `sales_asin_idq`, `sales_brand_idq`, (COALESCE(`sales_asin_idq`* `IDQ Score`,0)) AS `Sales Weight`, ('Creative') AS `Element`, (CASE WHEN (`Sales Weight` / `sales_brand_idq`) IS NULL THEN 0 ELSE (`Sales Weight` / `sales_brand_idq`) END) AS `IDQ Weight` FROM _Join_Data_1
),

_BB_Append AS (
  SELECT `country_id`, `brand name` AS `Brand`, `Element`, `Weighted Brand Buy Box` AS `Metric Value` FROM _Group_By_6
),

_BB_Join AS (
  SELECT `country_id`, `brand name` AS `Brand`, `Weighted Brand Buy Box` AS `Buy Box` FROM _Group_By_6
),

_Select_Columns_7 AS (
  SELECT `Brand`, `Element`, `Weighted Brand Avg Rating` AS `Metric Value`, `country_id` FROM _Group_By_12
),

_Select_Columns_13 AS (
  SELECT `Brand`, `Weighted Brand Avg Rating` AS `Avg Rating`, `country_id` FROM _Group_By_12
),

_Group_By_10 AS (
  SELECT `Brand`, `Element`, `country_id`, SUM(`Weighted Brand IDQ`) AS `Weighted Brand IDQ` FROM _IDQ GROUP BY `Brand`, `Element`, `country_id`
),

_Select_Columns_4 AS (
  SELECT `asin`, `IDQ Score` AS `Metric`, `brand name` AS `Brand`, `Sales Weight`, `IDQ Weight` AS `Metric Weight`, `sales_asin_idq` AS `Sales ASIN Weight`, `sales_brand_idq` AS `Sales Brand Weight` FROM _IDQ
),

_Join_Data_4 AS (
  SELECT _IS_Join.`country_id`, _IS_Join.`Brand`, _IS_Join.`In-Stock Rate`, _BB_Join.`Buy Box` FROM _IS_Join FULL JOIN _BB_Join ON _IS_Join.`Brand` = _BB_Join.`Brand` AND _IS_Join.`country_id` = _BB_Join.`country_id`
),

_IDQ_Append AS (
  SELECT `Brand`, `Element`, `Weighted Brand IDQ` AS `Metric Value`, `country_id` FROM _Group_By_10
),

_IDQ_Join AS (
  SELECT `Brand`, `Weighted Brand IDQ` AS `IDQ`, `country_id` FROM _Group_By_10
),

_Append_Rows AS (
  SELECT `country_id`, `asin`, `Sales ASIN Weight`, `Brand`, `Metric`, `Sales Weight`, `Metric Weight`, `Element`, `Sales Brand Weight` FROM _Select_Columns UNION ALL SELECT `country_id`, `asin`, `Sales ASIN Weight`, `Brand`, CAST(NULL AS DECIMAL) AS `Metric`, `Sales Weight`, `Metric Weight`, `Element`, `Sales Brand Weight` FROM _Select_Columns_2 UNION ALL SELECT CAST(NULL AS SIGNED) AS `country_id`, `asin`, `Sales ASIN Weight`, `Brand`, `Metric`, `Sales Weight`, `Metric Weight`, CAST(NULL AS VARCHAR) AS `Element`, `Sales Brand Weight` FROM _Select_Columns_4 UNION ALL SELECT CAST(NULL AS SIGNED) AS `country_id`, `asin`, `Sales ASIN Weight`, `Brand`, CAST(`Metric` AS DECIMAL) AS `Metric`, `Sales Weight`, `Metric Weight`, CAST(NULL AS VARCHAR) AS `Element`, `Sales Brand Weight` FROM _Select_Columns_5
),

_Append_Rows_1 AS (
  SELECT `country_id`, `Brand`, `Metric Value`, `Element` FROM _IS_Append UNION ALL SELECT `country_id`, `Brand`, `Metric Value`, `Element` FROM _BB_Append UNION ALL SELECT CAST(`country_id` AS SIGNED) AS `country_id`, `Brand`, `Metric Value`, `Element` FROM _IDQ_Append UNION ALL SELECT `country_id`, `Brand`, `Metric Value`, `Element` FROM _Select_Columns_7 UNION ALL SELECT CAST(NULL AS SIGNED) AS `country_id`, `Brand`, CAST(`Metric Value` AS DECIMAL) AS `Metric Value`, `Element` FROM _Select_Columns_8 UNION ALL SELECT CAST(`country_id` AS SIGNED) AS `country_id`, `Brand`, `Metric Value`, `Element` FROM _Select_Columns_11
),

_Join_Data_5 AS (
  SELECT _Join_Data_4.`country_id`, _Join_Data_4.`Brand`, _Join_Data_4.`In-Stock Rate`, _Join_Data_4.`Buy Box`, _IDQ_Join.`IDQ` FROM _Join_Data_4 FULL JOIN _IDQ_Join ON _Join_Data_4.`Brand` = _IDQ_Join.`Brand`
),

_Select_Columns_10 AS (
  SELECT `country_id`, `asin`, `Sales ASIN Weight`, `Brand` AS `brand name`, `Metric`, `Sales Weight`, `Metric Weight`, `Element`, `Sales Brand Weight`, `Number of Unique Days`, `Metric Value` FROM _Append_Rows
),

_Select_Columns_9 AS (
  SELECT `country_id`, `Brand` AS `brand name`, `Metric Value`, `Element` FROM _Append_Rows_1
),

_Join_Data_7 AS (
  SELECT _Join_Data_5.`country_id`, _Join_Data_5.`Brand`, _Join_Data_5.`In-Stock Rate`, _Join_Data_5.`Buy Box`, _Join_Data_5.`IDQ`, _Select_Columns_13.`Avg Rating` FROM _Join_Data_5 FULL JOIN _Select_Columns_13 ON _Join_Data_5.`Brand` = _Select_Columns_13.`Brand`
),

_Radar_ASIN_Level_Metrics AS (
  SELECT * FROM _Select_Columns_10
),

_Radar_BRAND_Level_Append_Metrics AS (
  SELECT * FROM _Select_Columns_9
),

_Join_Data_8 AS (
  SELECT _Join_Data_7.`country_id`, _Join_Data_7.`Brand`, _Join_Data_7.`In-Stock Rate`, _Join_Data_7.`Buy Box`, _Join_Data_7.`IDQ`, _Join_Data_7.`Avg Rating`, _Select_Columns_14.`Momentum`, _Select_Columns_14.`country_id` AS `Select Columns 14.country_id` FROM _Join_Data_7 LEFT JOIN _Select_Columns_14 ON _Join_Data_7.`Brand` = _Select_Columns_14.`Brand`
),

_Join_Data_9 AS (
  SELECT _Join_Data_8.`country_id`, _Join_Data_8.`Brand`, _Join_Data_8.`In-Stock Rate`, _Join_Data_8.`Buy Box`, _Join_Data_8.`IDQ`, _Join_Data_8.`Avg Rating`, _Join_Data_8.`Momentum`, _Join_Data_8.`Select Columns 14.country_id`, _Select_Columns_15.`Brand` AS `Select Columns 15.Brand`, _Select_Columns_15.`Ads`, _Select_Columns_15.`country_id` AS `Select Columns 15.country_id` FROM _Join_Data_8 FULL JOIN _Select_Columns_15 ON _Join_Data_8.`Brand` = _Select_Columns_15.`Brand`
),

_Radar_Brand_Level_Join_Metrics AS (
  SELECT * FROM _Join_Data_9
)

SELECT * FROM _Radar_Brand_Level_Join_Metrics