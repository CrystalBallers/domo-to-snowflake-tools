/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 720
Dataflow Name: Full Enforcement Dataflow
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 26
  Successful: 26
  Failed: 0
  Unique Action Types: 7
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, Metadata, PublishToVault
  Pipelines: 5

Generated: 2025-07-02 10:26:10
================================================================================
*/
WITH _Full_Limit_Buster_Inventory_DS AS (
  SELECT
    sales_country_id,
    revenue,
    asins_units,
    revenue_brand,
    asins_units_brand,
    percent_mix_of_brand,
    revenue_rank_r3mth,
    revenue_distribution_r3mth,
    scraped_date,
    amazonid,
    sellerid,
    sellername,
    storefront_name,
    asin,
    isquantityrestricted,
    brand,
    isfba,
    upc,
    upc_name,
    msrp,
    srp,
    map,
    parent_category,
    child_category,
    child_category_2,
    child_category_3,
    partner_name,
    country_id,
    offer_price,
    offer_date_max_price,
    units,
    limit_buster_units,
    redirect_asin,
    created_date,
    created_time,
    sku_map,
    sku_name,
    buy_box_count,
    partner_-_brand,
    cod_condition,
    unit_in_listing,
    bp_brand_id,
    asin_bp_enforceable,
    country,
    bbc_original_status,
    bbc_2_8_22_status
  FROM Full_Limit_Buster_Inventory_DS
), _BP_PlatformNotices_Sent AS (
  SELECT
    staff,
    brand,
    storefront_id,
    storefront_name,
    asin,
    date_of_first_message,
    complaint_date,
    complaint_id,
    message,
    column_10
  FROM BP_PlatformNotices_Sent
), _bp_enforcements AS (
  SELECT
    staff,
    partner,
    brand,
    marketplace_channel_id,
    marketplace_channel,
    country_id,
    country,
    storefront_id,
    storefront_name,
    date,
    type,
    notes_comments,
    invoice_id,
    job_id,
    usps_tracking,
    delivery_status,
    delivery_date,
    response_received,
    order_cost,
    usps_tracking_type,
    recipient_address,
    delivered_city,
    error_status,
    rundatetime,
    return_receipt_downloaded,
    enforce_by_brand,
    enforce_by,
    batch_id,
    batch_last_run
  FROM bp_enforcements
), _Keepa_Suppression_Status_Airtable AS (
  SELECT
    *
  FROM Keepa_Suppression_Status_Airtable
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
), _Max_Units AS (
  SELECT
    brand,
    sellerid,
    MAX(units) AS max_units
  FROM _Full_Limit_Buster_Inventory_DS
  GROUP BY
    brand,
    sellerid
), _Join_Data_6 AS (
  SELECT
    _bp_enforcements.staff,
    _bp_enforcements.partner,
    _bp_enforcements.brand,
    _bp_enforcements.marketplace_channel_id,
    _bp_enforcements.marketplace_channel,
    _bp_enforcements.country_id,
    _bp_enforcements.country,
    _bp_enforcements.storefront_id,
    _bp_enforcements.storefront_name,
    _bp_enforcements.date,
    _bp_enforcements.type,
    _bp_enforcements.notes_comments,
    _bp_enforcements.invoice_id,
    _bp_enforcements.job_id,
    _bp_enforcements.usps_tracking,
    _bp_enforcements.delivery_status,
    _bp_enforcements.delivery_date,
    _bp_enforcements.response_received,
    _bp_enforcements.order_cost,
    _bp_enforcements.usps_tracking_type,
    _bp_enforcements.recipient_address,
    _bp_enforcements.delivered_city,
    _bp_enforcements.error_status,
    _bp_enforcements.rundatetime,
    _bp_enforcements.return_receipt_downloaded,
    _bp_enforcements.enforce_by_brand,
    _bp_enforcements.enforce_by,
    _bp_enforcements.batch_id,
    _bp_enforcements.batch_last_run,
    _BP_PlatformNotices_Sent.asin,
    _BP_PlatformNotices_Sent.date_of_first_message,
    _BP_PlatformNotices_Sent.complaint_date,
    _BP_PlatformNotices_Sent.complaint_id,
    _BP_PlatformNotices_Sent.message,
    _BP_PlatformNotices_Sent.column_10
  FROM _bp_enforcements
  LEFT JOIN _BP_PlatformNotices_Sent
    ON _bp_enforcements.brand = _BP_PlatformNotices_Sent.brand
    AND _bp_enforcements.storefront_id = _BP_PlatformNotices_Sent.storefront_id
), _Filter_Rows AS (
  SELECT
    *
  FROM _bp_enforcements
  WHERE
    type = 'Cease & Desist'
), _Add_Formula_1 AS (
  SELECT
    *,
    (
      CASE
        WHEN country = 'USA'
        THEN 1
        WHEN country = 'Canada'
        THEN 2
        WHEN country = 'UK'
        THEN 3
        WHEN country = 'France'
        THEN 4
        WHEN country = 'Italy'
        THEN 5
        WHEN country = 'Germany'
        THEN 6
        WHEN country = 'Spain'
        THEN 7
        WHEN country = 'Netherlands'
        THEN 8
        WHEN country = 'Poland'
        THEN 9
        WHEN country = 'Sweden'
        THEN 10
        WHEN country = 'Belgium'
        THEN 11
        ELSE 0
      END
    ) AS country_id
  FROM _Keepa_Suppression_Status_Airtable
), _Fix_Vendor_SKU AS (
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
    batch_last_run,
    (
      CASE
        WHEN brand_id IN (75, 601, 602, 1270, 1936) AND LENGTH(vendor_sku) >= 5
        THEN NULL
        ELSE vendor_sku
      END
    ) AS vendor_sku_corrected
  FROM _UPCs
), _Join_Data_7 AS (
  SELECT
    _Max_Units.max_units,
    _Full_Limit_Buster_Inventory_DS.sales_country_id,
    _Full_Limit_Buster_Inventory_DS.revenue,
    _Full_Limit_Buster_Inventory_DS.asins_units,
    _Full_Limit_Buster_Inventory_DS.revenue_brand,
    _Full_Limit_Buster_Inventory_DS.asins_units_brand,
    _Full_Limit_Buster_Inventory_DS.percent_mix_of_brand,
    _Full_Limit_Buster_Inventory_DS.revenue_rank_r3mth,
    _Full_Limit_Buster_Inventory_DS.revenue_distribution_r3mth,
    _Full_Limit_Buster_Inventory_DS.scraped_date,
    _Full_Limit_Buster_Inventory_DS.amazonid,
    _Full_Limit_Buster_Inventory_DS.sellerid,
    _Full_Limit_Buster_Inventory_DS.sellername,
    _Full_Limit_Buster_Inventory_DS.storefront_name,
    _Full_Limit_Buster_Inventory_DS.asin,
    _Full_Limit_Buster_Inventory_DS.isquantityrestricted,
    _Full_Limit_Buster_Inventory_DS.brand,
    _Full_Limit_Buster_Inventory_DS.isfba,
    _Full_Limit_Buster_Inventory_DS.upc,
    _Full_Limit_Buster_Inventory_DS.upc_name,
    _Full_Limit_Buster_Inventory_DS.msrp,
    _Full_Limit_Buster_Inventory_DS.srp,
    _Full_Limit_Buster_Inventory_DS.map,
    _Full_Limit_Buster_Inventory_DS.parent_category,
    _Full_Limit_Buster_Inventory_DS.child_category,
    _Full_Limit_Buster_Inventory_DS.child_category_2,
    _Full_Limit_Buster_Inventory_DS.child_category_3,
    _Full_Limit_Buster_Inventory_DS.partner_name,
    _Full_Limit_Buster_Inventory_DS.country_id,
    _Full_Limit_Buster_Inventory_DS.offer_price,
    _Full_Limit_Buster_Inventory_DS.offer_date_max_price,
    _Full_Limit_Buster_Inventory_DS.units,
    _Full_Limit_Buster_Inventory_DS.limit_buster_units,
    _Full_Limit_Buster_Inventory_DS.redirect_asin,
    _Full_Limit_Buster_Inventory_DS.created_date,
    _Full_Limit_Buster_Inventory_DS.created_time,
    _Full_Limit_Buster_Inventory_DS.sku_map,
    _Full_Limit_Buster_Inventory_DS.sku_name,
    _Full_Limit_Buster_Inventory_DS.buy_box_count,
    _Full_Limit_Buster_Inventory_DS.partner_-_brand,
    _Full_Limit_Buster_Inventory_DS.cod_condition,
    _Full_Limit_Buster_Inventory_DS.unit_in_listing,
    _Full_Limit_Buster_Inventory_DS.bp_brand_id,
    _Full_Limit_Buster_Inventory_DS.asin_bp_enforceable,
    _Full_Limit_Buster_Inventory_DS.country,
    _Full_Limit_Buster_Inventory_DS.bbc_original_status,
    _Full_Limit_Buster_Inventory_DS.bbc_2_8_22_status
  FROM _Max_Units
  INNER JOIN _Full_Limit_Buster_Inventory_DS
    ON _Max_Units.brand = _Full_Limit_Buster_Inventory_DS.brand
    AND _Max_Units.sellerid = _Full_Limit_Buster_Inventory_DS.sellerid
    AND _Max_Units.max_units = _Full_Limit_Buster_Inventory_DS.units
), _Group_By_5 AS (
  SELECT
    brand,
    storefront_id,
    MAX(date) AS candd_date
  FROM _Filter_Rows
  GROUP BY
    brand,
    storefront_id
), _Alter_Columns AS (
  SELECT
    map AS map_airtable,
    country_id
  FROM _Add_Formula_1
), _Group_by_UPC AS (
  SELECT
    upc,
    MAX(vendor_sku_corrected) AS vendor_code,
    MAX(map) AS catalog_map
  FROM _Fix_Vendor_SKU
  GROUP BY
    upc
), _ASIN_with_highest_inventory AS (
  SELECT
    max_units,
    sellerid,
    brand,
    sales_country_id,
    MIN(asin) AS asin_with_highest_inventory
  FROM _Join_Data_7
  GROUP BY
    max_units,
    sellerid,
    brand,
    sales_country_id
), _Join_Data_9 AS (
  SELECT
    _Join_Data_6.staff,
    _Join_Data_6.brand,
    _Join_Data_6.storefront_id,
    _Join_Data_6.storefront_name,
    _Join_Data_6.asin,
    _Join_Data_6.date_of_first_message,
    _Join_Data_6.complaint_date,
    _Join_Data_6.complaint_id,
    _Join_Data_6.message,
    _Join_Data_6.column_10,
    _Join_Data_6.partner,
    _Join_Data_6.marketplace_channel_id,
    _Join_Data_6.marketplace_channel,
    _Join_Data_6.country_id,
    _Join_Data_6.country,
    _Join_Data_6.date,
    _Join_Data_6.type,
    _Join_Data_6.notes_comments,
    _Join_Data_6.invoice_id,
    _Join_Data_6.job_id,
    _Join_Data_6.usps_tracking,
    _Join_Data_6.delivery_status,
    _Join_Data_6.delivery_date,
    _Join_Data_6.response_received,
    _Join_Data_6.order_cost,
    _Join_Data_6.usps_tracking_type,
    _Join_Data_6.recipient_address,
    _Join_Data_6.delivered_city,
    _Join_Data_6.error_status,
    _Join_Data_6.rundatetime,
    _Join_Data_6.return_receipt_downloaded,
    _Join_Data_6.enforce_by_brand,
    _Join_Data_6.enforce_by,
    _Join_Data_6.batch_id,
    _Join_Data_6.batch_last_run,
    _Group_By_5.candd_date
  FROM _Join_Data_6
  LEFT JOIN _Group_By_5
    ON _Join_Data_6.brand = _Group_By_5.brand
    AND _Join_Data_6.storefront_id = _Group_By_5.storefront_id
), _Group_By AS (
  SELECT
    brand,
    storefront_id,
    MAX(type) AS last_notice_type,
    MAX(date) AS last_notice_sent,
    MAX(notes_comments) AS last_note,
    MAX(invoice_id) AS invoice_id,
    MAX(job_id) AS job_id,
    MAX(usps_tracking) AS usps_tracking,
    MAX(delivery_status) AS delivery_status,
    MAX(delivery_date) AS delivery_date,
    LISTAGG(type, '') AS enforcement_history,
    MAX(error_status) AS c2m_error_status,
    MAX(recipient_address) AS recipient_address,
    MAX(date_of_first_message) AS platform_sent,
    LISTAGG(notes_comments, '') AS notes,
    MAX(candd_date) AS candd_date
  FROM _Join_Data_9
  GROUP BY
    brand,
    storefront_id
), _Join_Data AS (
  SELECT
    _Full_Limit_Buster_Inventory_DS.sales_country_id,
    _Full_Limit_Buster_Inventory_DS.revenue,
    _Full_Limit_Buster_Inventory_DS.asins_units,
    _Full_Limit_Buster_Inventory_DS.revenue_brand,
    _Full_Limit_Buster_Inventory_DS.asins_units_brand,
    _Full_Limit_Buster_Inventory_DS.percent_mix_of_brand,
    _Full_Limit_Buster_Inventory_DS.revenue_rank_r3mth,
    _Full_Limit_Buster_Inventory_DS.revenue_distribution_r3mth,
    _Full_Limit_Buster_Inventory_DS.scraped_date,
    _Full_Limit_Buster_Inventory_DS.amazonid,
    _Full_Limit_Buster_Inventory_DS.sellerid,
    _Full_Limit_Buster_Inventory_DS.sellername,
    _Full_Limit_Buster_Inventory_DS.storefront_name,
    _Full_Limit_Buster_Inventory_DS.asin,
    _Full_Limit_Buster_Inventory_DS.isquantityrestricted,
    _Full_Limit_Buster_Inventory_DS.brand,
    _Full_Limit_Buster_Inventory_DS.isfba,
    _Full_Limit_Buster_Inventory_DS.upc,
    _Full_Limit_Buster_Inventory_DS.upc_name,
    _Full_Limit_Buster_Inventory_DS.msrp,
    _Full_Limit_Buster_Inventory_DS.srp,
    _Full_Limit_Buster_Inventory_DS.map,
    _Full_Limit_Buster_Inventory_DS.parent_category,
    _Full_Limit_Buster_Inventory_DS.child_category,
    _Full_Limit_Buster_Inventory_DS.child_category_2,
    _Full_Limit_Buster_Inventory_DS.child_category_3,
    _Full_Limit_Buster_Inventory_DS.partner_name,
    _Full_Limit_Buster_Inventory_DS.country_id,
    _Full_Limit_Buster_Inventory_DS.offer_price,
    _Full_Limit_Buster_Inventory_DS.offer_date_max_price,
    _Full_Limit_Buster_Inventory_DS.units,
    _Full_Limit_Buster_Inventory_DS.limit_buster_units,
    _Full_Limit_Buster_Inventory_DS.redirect_asin,
    _Full_Limit_Buster_Inventory_DS.created_date,
    _Full_Limit_Buster_Inventory_DS.created_time,
    _Full_Limit_Buster_Inventory_DS.sku_map,
    _Full_Limit_Buster_Inventory_DS.sku_name,
    _Full_Limit_Buster_Inventory_DS.buy_box_count,
    _Full_Limit_Buster_Inventory_DS.partner_-_brand,
    _Full_Limit_Buster_Inventory_DS.cod_condition,
    _Full_Limit_Buster_Inventory_DS.unit_in_listing,
    _Full_Limit_Buster_Inventory_DS.bp_brand_id,
    _Full_Limit_Buster_Inventory_DS.asin_bp_enforceable,
    _Full_Limit_Buster_Inventory_DS.country,
    _Full_Limit_Buster_Inventory_DS.bbc_original_status,
    _Full_Limit_Buster_Inventory_DS.bbc_2_8_22_status,
    _Group_By.storefront_id,
    _Group_By.last_notice_type,
    _Group_By.last_notice_sent,
    _Group_By.last_note,
    _Group_By.invoice_id,
    _Group_By.job_id,
    _Group_By.usps_tracking,
    _Group_By.delivery_status,
    _Group_By.delivery_date,
    _Group_By.enforcement_history,
    _Group_By.c2m_error_status,
    _Group_By.recipient_address,
    _Group_By.platform_sent,
    _Group_By.notes,
    _Group_By.candd_date
  FROM _Full_Limit_Buster_Inventory_DS
  LEFT JOIN _Group_By
    ON _Full_Limit_Buster_Inventory_DS.brand = _Group_By.brand
    AND _Full_Limit_Buster_Inventory_DS.sellerid = _Group_By.storefront_id
), _Group_By_1 AS (
  SELECT
    brand,
    amazonid,
    MIN(scraped_date) AS first_seen_date,
    MAX(scraped_date) AS last_seen_date
  FROM _Join_Data
  GROUP BY
    brand,
    amazonid
), _Join_Data_1 AS (
  SELECT
    _Join_Data.sales_country_id,
    _Join_Data.revenue,
    _Join_Data.asins_units,
    _Join_Data.revenue_brand,
    _Join_Data.asins_units_brand,
    _Join_Data.percent_mix_of_brand,
    _Join_Data.revenue_rank_r3mth,
    _Join_Data.revenue_distribution_r3mth,
    _Join_Data.scraped_date,
    _Join_Data.amazonid,
    _Join_Data.sellerid,
    _Join_Data.sellername,
    _Join_Data.storefront_name,
    _Join_Data.asin,
    _Join_Data.isquantityrestricted,
    _Join_Data.brand,
    _Join_Data.isfba,
    _Join_Data.upc,
    _Join_Data.upc_name,
    _Join_Data.msrp,
    _Join_Data.srp,
    _Join_Data.map,
    _Join_Data.parent_category,
    _Join_Data.child_category,
    _Join_Data.child_category_2,
    _Join_Data.child_category_3,
    _Join_Data.partner_name,
    _Join_Data.country_id,
    _Join_Data.offer_price,
    _Join_Data.offer_date_max_price,
    _Join_Data.units,
    _Join_Data.limit_buster_units,
    _Join_Data.redirect_asin,
    _Join_Data.created_date,
    _Join_Data.created_time,
    _Join_Data.sku_map,
    _Join_Data.sku_name,
    _Join_Data.buy_box_count,
    _Join_Data.partner_-_brand,
    _Join_Data.cod_condition,
    _Join_Data.unit_in_listing,
    _Join_Data.bp_brand_id,
    _Join_Data.asin_bp_enforceable,
    _Join_Data.country,
    _Join_Data.bbc_original_status,
    _Join_Data.bbc_2_8_22_status,
    _Join_Data.storefront_id,
    _Join_Data.last_notice_type,
    _Join_Data.last_notice_sent,
    _Join_Data.last_note,
    _Join_Data.invoice_id,
    _Join_Data.job_id,
    _Join_Data.usps_tracking,
    _Join_Data.delivery_status,
    _Join_Data.delivery_date,
    _Join_Data.enforcement_history,
    _Join_Data.c2m_error_status,
    _Join_Data.recipient_address,
    _Join_Data.platform_sent,
    _Join_Data.notes,
    _Join_Data.candd_date,
    _Group_By_1.first_seen_date,
    _Group_By_1.last_seen_date
  FROM _Join_Data
  LEFT JOIN _Group_By_1
    ON _Join_Data.amazonid = _Group_By_1.amazonid
    AND _Join_Data.brand = _Group_By_1.brand
), _Add_Formula AS (
  SELECT
    brand,
    amazonid,
    first_seen_date,
    last_seen_date,
    sales_country_id,
    revenue,
    asins_units,
    revenue_brand,
    asins_units_brand,
    percent_mix_of_brand,
    revenue_rank_r3mth,
    revenue_distribution_r3mth,
    scraped_date,
    sellerid,
    sellername,
    storefront_name,
    asin,
    isquantityrestricted,
    isfba,
    upc,
    upc_name,
    msrp,
    srp,
    map,
    parent_category,
    child_category,
    child_category_2,
    child_category_3,
    partner_name,
    country_id,
    offer_price,
    offer_date_max_price,
    units,
    limit_buster_units,
    redirect_asin,
    created_date,
    created_time,
    sku_map,
    sku_name,
    buy_box_count,
    partner_-_brand,
    cod_condition,
    unit_in_listing,
    bp_brand_id,
    asin_bp_enforceable,
    country,
    bbc_original_status,
    bbc_2_8_22_status,
    storefront_id,
    last_notice_type,
    last_notice_sent,
    last_note,
    invoice_id,
    job_id,
    usps_tracking,
    delivery_status,
    delivery_date,
    enforcement_history,
    c2m_error_status,
    recipient_address,
    platform_sent,
    notes,
    candd_date,
    (
      CASE
        WHEN first_seen_date >= DATEADD(DAY, '7' * -1, CURRENT_DATE)
        THEN 1
        ELSE 0
      END
    ) AS new_storefront_within_7_days,
    (
      DATEDIFF(DAY, CURRENT_DATE, first_seen_date)
    ) AS days_since_first_listing,
    (
      DATEDIFF(DAY, CURRENT_DATE, last_seen_date)
    ) AS days_since_last_listing,
    (
      CASE
        WHEN first_seen_date >= DATEADD(DAY, '1' * -1, CURRENT_DATE)
        THEN 1
        ELSE 0
      END
    ) AS new_storefront_within_2_days,
    (
      CASE
        WHEN first_seen_date >= DATEADD(DAY, '14' * -1, CURRENT_DATE)
        THEN 1
        ELSE 0
      END
    ) AS new_storefront_within_14_days,
    (
      CASE
        WHEN first_seen_date >= DATEADD(DAY, '28' * -1, CURRENT_DATE)
        THEN 1
        ELSE 0
      END
    ) AS new_storefront_within_28_days
  FROM _Join_Data_1
), _Add_Vendor_Code AS (
  SELECT
    _Add_Formula.brand,
    _Add_Formula.amazonid,
    _Add_Formula.first_seen_date,
    _Add_Formula.last_seen_date,
    _Add_Formula.sales_country_id,
    _Add_Formula.revenue,
    _Add_Formula.asins_units,
    _Add_Formula.revenue_brand,
    _Add_Formula.asins_units_brand,
    _Add_Formula.percent_mix_of_brand,
    _Add_Formula.revenue_rank_r3mth,
    _Add_Formula.revenue_distribution_r3mth,
    _Add_Formula.scraped_date,
    _Add_Formula.sellerid,
    _Add_Formula.sellername,
    _Add_Formula.storefront_name,
    _Add_Formula.asin,
    _Add_Formula.isquantityrestricted,
    _Add_Formula.isfba,
    _Add_Formula.upc,
    _Add_Formula.upc_name,
    _Add_Formula.msrp,
    _Add_Formula.srp,
    _Add_Formula.map,
    _Add_Formula.parent_category,
    _Add_Formula.child_category,
    _Add_Formula.child_category_2,
    _Add_Formula.child_category_3,
    _Add_Formula.partner_name,
    _Add_Formula.country_id,
    _Add_Formula.offer_price,
    _Add_Formula.offer_date_max_price,
    _Add_Formula.units,
    _Add_Formula.limit_buster_units,
    _Add_Formula.redirect_asin,
    _Add_Formula.created_date,
    _Add_Formula.created_time,
    _Add_Formula.sku_map,
    _Add_Formula.sku_name,
    _Add_Formula.buy_box_count,
    _Add_Formula.partner_-_brand,
    _Add_Formula.cod_condition,
    _Add_Formula.unit_in_listing,
    _Add_Formula.bp_brand_id,
    _Add_Formula.asin_bp_enforceable,
    _Add_Formula.country,
    _Add_Formula.bbc_original_status,
    _Add_Formula.bbc_2_8_22_status,
    _Add_Formula.storefront_id,
    _Add_Formula.last_notice_type,
    _Add_Formula.last_notice_sent,
    _Add_Formula.last_note,
    _Add_Formula.invoice_id,
    _Add_Formula.job_id,
    _Add_Formula.usps_tracking,
    _Add_Formula.delivery_status,
    _Add_Formula.delivery_date,
    _Add_Formula.enforcement_history,
    _Add_Formula.c2m_error_status,
    _Add_Formula.recipient_address,
    _Add_Formula.platform_sent,
    _Add_Formula.notes,
    _Add_Formula.candd_date,
    _Add_Formula.new_storefront_within_7_days,
    _Add_Formula.days_since_first_listing,
    _Add_Formula.days_since_last_listing,
    _Add_Formula.new_storefront_within_2_days,
    _Add_Formula.new_storefront_within_14_days,
    _Add_Formula.new_storefront_within_28_days,
    _Group_by_UPC.vendor_code AS vendor_code,
    _Group_by_UPC.catalog_map
  FROM _Add_Formula
  LEFT JOIN _Group_by_UPC
    ON _Add_Formula.upc = _Group_by_UPC.upc
), _Join_Data_8 AS (
  SELECT
    _ASIN_with_highest_inventory.max_units,
    _ASIN_with_highest_inventory.asin_with_highest_inventory,
    _Add_Vendor_Code.upc,
    _Add_Vendor_Code.vendor_code,
    _Add_Vendor_Code.catalog_map,
    _Add_Vendor_Code.brand,
    _Add_Vendor_Code.amazonid,
    _Add_Vendor_Code.first_seen_date,
    _Add_Vendor_Code.last_seen_date,
    _Add_Vendor_Code.sales_country_id,
    _Add_Vendor_Code.revenue,
    _Add_Vendor_Code.asins_units,
    _Add_Vendor_Code.revenue_brand,
    _Add_Vendor_Code.asins_units_brand,
    _Add_Vendor_Code.percent_mix_of_brand,
    _Add_Vendor_Code.revenue_rank_r3mth,
    _Add_Vendor_Code.revenue_distribution_r3mth,
    _Add_Vendor_Code.scraped_date,
    _Add_Vendor_Code.sellerid,
    _Add_Vendor_Code.sellername,
    _Add_Vendor_Code.storefront_name,
    _Add_Vendor_Code.asin,
    _Add_Vendor_Code.isquantityrestricted,
    _Add_Vendor_Code.isfba,
    _Add_Vendor_Code.upc_name,
    _Add_Vendor_Code.msrp,
    _Add_Vendor_Code.srp,
    _Add_Vendor_Code.map,
    _Add_Vendor_Code.parent_category,
    _Add_Vendor_Code.child_category,
    _Add_Vendor_Code.child_category_2,
    _Add_Vendor_Code.child_category_3,
    _Add_Vendor_Code.partner_name,
    _Add_Vendor_Code.country_id,
    _Add_Vendor_Code.offer_price,
    _Add_Vendor_Code.offer_date_max_price,
    _Add_Vendor_Code.units,
    _Add_Vendor_Code.limit_buster_units,
    _Add_Vendor_Code.redirect_asin,
    _Add_Vendor_Code.created_date,
    _Add_Vendor_Code.created_time,
    _Add_Vendor_Code.sku_map,
    _Add_Vendor_Code.sku_name,
    _Add_Vendor_Code.buy_box_count,
    _Add_Vendor_Code.partner_-_brand,
    _Add_Vendor_Code.cod_condition,
    _Add_Vendor_Code.unit_in_listing,
    _Add_Vendor_Code.bp_brand_id,
    _Add_Vendor_Code.asin_bp_enforceable,
    _Add_Vendor_Code.country,
    _Add_Vendor_Code.bbc_original_status,
    _Add_Vendor_Code.bbc_2_8_22_status,
    _Add_Vendor_Code.storefront_id,
    _Add_Vendor_Code.last_notice_type,
    _Add_Vendor_Code.last_notice_sent,
    _Add_Vendor_Code.last_note,
    _Add_Vendor_Code.invoice_id,
    _Add_Vendor_Code.job_id,
    _Add_Vendor_Code.usps_tracking,
    _Add_Vendor_Code.delivery_status,
    _Add_Vendor_Code.delivery_date,
    _Add_Vendor_Code.enforcement_history,
    _Add_Vendor_Code.c2m_error_status,
    _Add_Vendor_Code.recipient_address,
    _Add_Vendor_Code.platform_sent,
    _Add_Vendor_Code.notes,
    _Add_Vendor_Code.candd_date,
    _Add_Vendor_Code.new_storefront_within_7_days,
    _Add_Vendor_Code.days_since_first_listing,
    _Add_Vendor_Code.days_since_last_listing,
    _Add_Vendor_Code.new_storefront_within_2_days,
    _Add_Vendor_Code.new_storefront_within_14_days,
    _Add_Vendor_Code.new_storefront_within_28_days
  FROM _ASIN_with_highest_inventory
  RIGHT JOIN _Add_Vendor_Code
    ON _ASIN_with_highest_inventory.sellerid = _Add_Vendor_Code.sellerid
    AND _ASIN_with_highest_inventory.brand = _Add_Vendor_Code.brand
    AND _ASIN_with_highest_inventory.sales_country_id = _Add_Vendor_Code.sales_country_id
), _Add_Formula_4 AS (
  SELECT
    max_units,
    asin_with_highest_inventory,
    upc,
    vendor_code,
    catalog_map,
    brand,
    amazonid,
    first_seen_date,
    last_seen_date,
    sales_country_id,
    revenue,
    asins_units,
    revenue_brand,
    asins_units_brand,
    percent_mix_of_brand,
    revenue_rank_r3mth,
    revenue_distribution_r3mth,
    scraped_date,
    sellerid,
    sellername,
    storefront_name,
    asin,
    isquantityrestricted,
    isfba,
    upc_name,
    msrp,
    srp,
    map,
    parent_category,
    child_category,
    child_category_2,
    child_category_3,
    partner_name,
    country_id,
    offer_price,
    offer_date_max_price,
    units,
    limit_buster_units,
    redirect_asin,
    created_date,
    created_time,
    sku_map,
    sku_name,
    buy_box_count,
    partner_-_brand,
    cod_condition,
    unit_in_listing,
    bp_brand_id,
    asin_bp_enforceable,
    country,
    bbc_original_status,
    bbc_2_8_22_status,
    storefront_id,
    last_notice_type,
    last_notice_sent,
    last_note,
    invoice_id,
    job_id,
    usps_tracking,
    delivery_status,
    delivery_date,
    enforcement_history,
    c2m_error_status,
    recipient_address,
    platform_sent,
    notes,
    candd_date,
    new_storefront_within_7_days,
    days_since_first_listing,
    days_since_last_listing,
    new_storefront_within_2_days,
    new_storefront_within_14_days,
    new_storefront_within_28_days,
    (
      CASE WHEN sellername = 'XXXXXXXXX' THEN CURRENT_DATE ELSE '' END
    ) AS enter_date_new,
    (
      CASE WHEN sellername = 'XXXXXXXXX' THEN CURRENT_DATE ELSE '' END
    ) AS new_notes_new,
    (
      CASE WHEN sellername = 'XXXXXXXXX' THEN CURRENT_DATE ELSE '' END
    ) AS new_invoice_id_new
  FROM _Join_Data_8
), _Join_Data_2 AS (
  SELECT
    _Add_Formula_4.max_units,
    _Add_Formula_4.asin_with_highest_inventory,
    _Add_Formula_4.upc,
    _Add_Formula_4.vendor_code,
    _Add_Formula_4.catalog_map,
    _Add_Formula_4.brand,
    _Add_Formula_4.amazonid,
    _Add_Formula_4.first_seen_date,
    _Add_Formula_4.last_seen_date,
    _Add_Formula_4.sales_country_id,
    _Add_Formula_4.revenue,
    _Add_Formula_4.asins_units,
    _Add_Formula_4.revenue_brand,
    _Add_Formula_4.asins_units_brand,
    _Add_Formula_4.percent_mix_of_brand,
    _Add_Formula_4.revenue_rank_r3mth,
    _Add_Formula_4.revenue_distribution_r3mth,
    _Add_Formula_4.scraped_date,
    _Add_Formula_4.sellerid,
    _Add_Formula_4.sellername,
    _Add_Formula_4.storefront_name,
    _Add_Formula_4.asin,
    _Add_Formula_4.isquantityrestricted,
    _Add_Formula_4.isfba,
    _Add_Formula_4.upc_name,
    _Add_Formula_4.msrp,
    _Add_Formula_4.srp,
    _Add_Formula_4.map,
    _Add_Formula_4.parent_category,
    _Add_Formula_4.child_category,
    _Add_Formula_4.child_category_2,
    _Add_Formula_4.child_category_3,
    _Add_Formula_4.partner_name,
    _Add_Formula_4.country_id,
    _Add_Formula_4.offer_price,
    _Add_Formula_4.offer_date_max_price,
    _Add_Formula_4.units,
    _Add_Formula_4.limit_buster_units,
    _Add_Formula_4.redirect_asin,
    _Add_Formula_4.created_date,
    _Add_Formula_4.created_time,
    _Add_Formula_4.sku_map,
    _Add_Formula_4.sku_name,
    _Add_Formula_4.buy_box_count,
    _Add_Formula_4.partner_-_brand,
    _Add_Formula_4.cod_condition,
    _Add_Formula_4.unit_in_listing,
    _Add_Formula_4.bp_brand_id,
    _Add_Formula_4.asin_bp_enforceable,
    _Add_Formula_4.country,
    _Add_Formula_4.bbc_original_status,
    _Add_Formula_4.bbc_2_8_22_status,
    _Add_Formula_4.storefront_id,
    _Add_Formula_4.last_notice_type,
    _Add_Formula_4.last_notice_sent,
    _Add_Formula_4.last_note,
    _Add_Formula_4.invoice_id,
    _Add_Formula_4.job_id,
    _Add_Formula_4.usps_tracking,
    _Add_Formula_4.delivery_status,
    _Add_Formula_4.delivery_date,
    _Add_Formula_4.enforcement_history,
    _Add_Formula_4.c2m_error_status,
    _Add_Formula_4.recipient_address,
    _Add_Formula_4.platform_sent,
    _Add_Formula_4.notes,
    _Add_Formula_4.candd_date,
    _Add_Formula_4.new_storefront_within_7_days,
    _Add_Formula_4.days_since_first_listing,
    _Add_Formula_4.days_since_last_listing,
    _Add_Formula_4.new_storefront_within_2_days,
    _Add_Formula_4.new_storefront_within_14_days,
    _Add_Formula_4.new_storefront_within_28_days,
    _Add_Formula_4.enter_date_new,
    _Add_Formula_4.new_notes_new,
    _Add_Formula_4.new_invoice_id_new,
    _Alter_Columns.country_id
  FROM _Add_Formula_4
  LEFT JOIN _Alter_Columns
    ON _Add_Formula_4.asin = _Alter_Columns.asin
    AND _Add_Formula_4.sales_country_id = _Alter_Columns.country_id
), _Full_Enforcement_DS AS (
  SELECT
    *
  FROM _Join_Data_2
)
SELECT
  *
FROM _Full_Enforcement_DS