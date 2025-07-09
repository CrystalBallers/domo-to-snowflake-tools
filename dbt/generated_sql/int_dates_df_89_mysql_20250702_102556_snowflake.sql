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
WITH _sales_by_products_archive AS (
  SELECT
    brand_id,
    client_id,
    cogs,
    country_id,
    date,
    expanded_units,
    id,
    product_id,
    sales,
    units,
    brand_name,
    client_name,
    amazon_seller_id,
    country_1,
    batch_id,
    batch_last_run
  FROM sales_by_products_archive
), _Select_Columns_1 AS (
  SELECT
    brand_name,
    client_name,
    date,
    brand_id,
    client_id
  FROM _sales_by_products_archive
), _Remove_Duplicates AS (
  SELECT DISTINCT
    *
  FROM _Select_Columns_1
), _Add_Formula AS (
  SELECT
    brand_name,
    client_name,
    date,
    brand_id,
    client_id,
    (
      WEEK(TO_DATE(date))
    ) AS week_number,
    (
      CASE
        WHEN (
          WEEKOFYEAR(TO_DATE(date)) = 53
        )
        THEN YEAR(TO_DATE(date)) - 1
        ELSE YEAR(TO_DATE(date))
      END
    ) AS year,
    (
      CONCAT(
        CASE
          WHEN (
            WEEKOFYEAR(TO_DATE(date)) = 53
          )
          THEN YEAR(TO_DATE(date)) - 1
          ELSE YEAR(TO_DATE(date))
        END,
        '-',
        WEEKOFYEAR(TO_DATE(date))
      )
    ) AS week_code,
    (
      DATEADD(DAY, (
        7 - DAYOFWEEK(TO_DATE(date))
      ), date)
    ) AS end_week,
    (
      DATEADD(DAY, '6' * -1, DATEADD(DAY, (
        7 - DAYOFWEEK(TO_DATE(date))
      ), date))
    ) AS start_week
  FROM _Remove_Duplicates
), _Dates_DS AS (
  SELECT
    *
  FROM _Add_Formula
)
SELECT
  *
FROM _Dates_DS