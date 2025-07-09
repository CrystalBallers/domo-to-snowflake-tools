/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 89
Dataflow Name: Dates DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 5
  Successful: 5
  Failed: 0
  Unique Action Types: 5
  Action Types: ExpressionEvaluator, LoadFromVault, PublishToVault, SelectValues, Unique

Generated: 2025-07-02 10:25:56
================================================================================
*/

WITH
_sales_by_products_archive AS (
  SELECT `brand_id`, `client_id`, `cogs`, `country_id`, `date`, `expanded_units`, `id`, `product_id`, `sales`, `units`, `brand_name`, `client_name`, `amazon_seller_id`, `country_1`, `_BATCH_ID_`, `_BATCH_LAST_RUN_` FROM sales_by_products_archive
),
_Select_Columns_1 AS (
  SELECT `brand_name`, `client_name`, `date`, `brand_id`, `client_id` FROM _sales_by_products_archive
),
_Remove_Duplicates AS (
  SELECT DISTINCT * FROM _Select_Columns_1
),
_Add_Formula AS (
  SELECT `brand_name`, `client_name`, `date`, `brand_id`, `client_id`, (WEEK(`date`)) AS `week_number`, (CASE WHEN (WEEKOFYEAR(`date`) = 53) THEN year(`date`) -1 ELSE year(`date`) END) AS `Year`, (CONCAT(CASE WHEN (WEEKOFYEAR(`date`) = 53) THEN year(`date`) -1 ELSE year(`date`) END,'-' ,WEEKOFYEAR(`date`))) AS `week_code`, (DATE_ADD(`date`,INTERVAL (7-DAYOFWEEK(`date`)) DAY)) AS `end_week`, (Date_SUB(DATE_ADD(`date`,INTERVAL (7-DAYOFWEEK(`date`)) DAY), INTERVAL 6 DAY)) AS `start_week` FROM _Remove_Duplicates
),
_Dates_DS AS (
  SELECT * FROM _Add_Formula
)
SELECT * FROM _Dates_DS