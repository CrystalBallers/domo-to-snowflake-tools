/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 487
Dataflow Name: Full Enforcement Yesterday Dataflow
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 24
  Successful: 24
  Failed: 0
  Unique Action Types: 7
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, PublishToVault, SelectValues
  Pipelines: 6

Generated: 2025-07-02 10:24:04
================================================================================
*/
WITH _bp_enforcement_settings AS (
  SELECT
    bp_partner_id,
    bp_brand_id,
    bp_partner,
    bp_brand,
    settings_by,
    enforce_by,
    country_id,
    country_name,
    country_code,
    marketplace_channel_id,
    marketplace_channel,
    enforcement_configuration,
    offers_scraper,
    suppression_tracking,
    is_bp_enforceable,
    offers_scraper_enabled,
    suppression_tracking_enabled,
    assigned_user_id,
    assigned_user,
    assigned_user_initials,
    count_asin,
    enforcement_codes,
    batch_id,
    batch_last_run
  FROM bp_enforcement_settings
), _bp_amazon_seller_brands AS (
  SELECT
    seller_id,
    bp_partner_id,
    bp_brand_id,
    enforcement_configuration_id,
    comments,
    last_scraped_at,
    last_scraped_date,
    entity_id,
    enforcement_configuration_code,
    enforcement_configuration,
    bp_partner,
    bp_brand,
    assigned_user_id,
    assigned_user,
    assigned_user_initials,
    batch_id,
    batch_last_run
  FROM bp_amazon_seller_brands
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
), _Full_Limit_Buster_Inventory_DS_Yesterday AS (
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
  FROM Full_Limit_Buster_Inventory_DS_Yesterday
), _countries AS (
  SELECT
    created_at,
    currency,
    enabled,
    id,
    marketplace_id,
    name,
    sales_channel,
    short_name,
    updated_at,
    batch_id,
    batch_last_run
  FROM countries
), _Join_Data_4 AS (
  SELECT
    _bp_enforcement_settings.bp_partner_id,
    _bp_enforcement_settings.bp_brand_id,
    _bp_enforcement_settings.bp_partner,
    _bp_enforcement_settings.bp_brand,
    _bp_enforcement_settings.settings_by,
    _bp_enforcement_settings.enforce_by,
    _bp_enforcement_settings.country_id,
    _bp_enforcement_settings.country_name,
    _bp_enforcement_settings.country_code,
    _bp_enforcement_settings.marketplace_channel_id,
    _bp_enforcement_settings.marketplace_channel,
    _bp_enforcement_settings.enforcement_configuration AS brand_enforceable,
    _bp_enforcement_settings.offers_scraper,
    _bp_enforcement_settings.suppression_tracking,
    _bp_enforcement_settings.is_bp_enforceable,
    _bp_enforcement_settings.offers_scraper_enabled,
    _bp_enforcement_settings.suppression_tracking_enabled,
    _bp_enforcement_settings.assigned_user_id,
    _bp_enforcement_settings.assigned_user,
    _bp_enforcement_settings.assigned_user_initials,
    _bp_enforcement_settings.count_asin,
    _bp_enforcement_settings.enforcement_codes,
    _bp_enforcement_settings.batch_id,
    _bp_enforcement_settings.batch_last_run,
    _bp_amazon_seller_brands.seller_id,
    _bp_amazon_seller_brands.enforcement_configuration_id,
    _bp_amazon_seller_brands.comments,
    _bp_amazon_seller_brands.last_scraped_at,
    _bp_amazon_seller_brands.last_scraped_date,
    _bp_amazon_seller_brands.entity_id,
    _bp_amazon_seller_brands.enforcement_configuration_code,
    _bp_amazon_seller_brands.enforcement_configuration AS storefront_enforceable
  FROM _bp_enforcement_settings
  LEFT JOIN _bp_amazon_seller_brands
    ON _bp_enforcement_settings.bp_brand_id = _bp_amazon_seller_brands.bp_brand_id
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
), _Group_By AS (
  SELECT
    brand,
    storefront_id,
    marketplace_channel_id,
    marketplace_channel,
    country_id,
    country,
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
    LISTAGG(notes_comments, '') AS notes
  FROM _bp_enforcements
  GROUP BY
    brand,
    storefront_id,
    marketplace_channel_id,
    marketplace_channel,
    country_id,
    country
), _add_Channel AS (
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
    bbc_2_8_22_status,
    (
      1
    ) AS marketplace_channel_id,
    (
      'Amazon'
    ) AS marketplace_channel
  FROM _Full_Limit_Buster_Inventory_DS_Yesterday
), _Select_name AS (
  SELECT
    id,
    name AS country
  FROM _countries
), _seller_brands_cols AS (
  SELECT
    seller_id,
    enforcement_configuration_id,
    bp_brand_id,
    assigned_user AS brand_assigned_user,
    assigned_user_initials AS brand_assigned_user_initials,
    storefront_enforceable,
    brand_enforceable,
    assigned_user_id,
    country_id
  FROM _Join_Data_4
), _Group_by_UPC AS (
  SELECT
    upc,
    MAX(vendor_sku_corrected) AS vendor_code,
    MAX(map) AS catalog_map
  FROM _Fix_Vendor_SKU
  GROUP BY
    upc
), _limit_buster_country AS (
  SELECT
    _add_Channel.sales_country_id,
    _add_Channel.revenue,
    _add_Channel.asins_units,
    _add_Channel.revenue_brand,
    _add_Channel.asins_units_brand,
    _add_Channel.percent_mix_of_brand,
    _add_Channel.revenue_rank_r3mth,
    _add_Channel.revenue_distribution_r3mth,
    _add_Channel.scraped_date,
    _add_Channel.amazonid,
    _add_Channel.sellerid,
    _add_Channel.sellername,
    _add_Channel.storefront_name,
    _add_Channel.asin,
    _add_Channel.isquantityrestricted,
    _add_Channel.brand,
    _add_Channel.isfba,
    _add_Channel.upc,
    _add_Channel.upc_name,
    _add_Channel.msrp,
    _add_Channel.srp,
    _add_Channel.map,
    _add_Channel.parent_category,
    _add_Channel.child_category,
    _add_Channel.child_category_2,
    _add_Channel.child_category_3,
    _add_Channel.partner_name,
    _add_Channel.country_id,
    _add_Channel.offer_price,
    _add_Channel.offer_date_max_price,
    _add_Channel.units,
    _add_Channel.limit_buster_units,
    _add_Channel.redirect_asin,
    _add_Channel.created_date,
    _add_Channel.created_time,
    _add_Channel.sku_map,
    _add_Channel.sku_name,
    _add_Channel.buy_box_count,
    _add_Channel.partner_-_brand,
    _add_Channel.cod_condition,
    _add_Channel.unit_in_listing,
    _add_Channel.bp_brand_id,
    _add_Channel.asin_bp_enforceable,
    _add_Channel.country,
    _add_Channel.bbc_original_status,
    _add_Channel.bbc_2_8_22_status,
    _add_Channel.marketplace_channel_id,
    _add_Channel.marketplace_channel,
    _Select_name.id
  FROM _add_Channel
  INNER JOIN _Select_name
    ON _add_Channel.country_id = _Select_name.id
), _Join_Data AS (
  SELECT
    _limit_buster_country.sales_country_id,
    _limit_buster_country.revenue,
    _limit_buster_country.asins_units,
    _limit_buster_country.revenue_brand,
    _limit_buster_country.asins_units_brand,
    _limit_buster_country.percent_mix_of_brand,
    _limit_buster_country.revenue_rank_r3mth,
    _limit_buster_country.revenue_distribution_r3mth,
    _limit_buster_country.scraped_date,
    _limit_buster_country.amazonid,
    _limit_buster_country.sellerid,
    _limit_buster_country.sellername,
    _limit_buster_country.storefront_name,
    _limit_buster_country.asin,
    _limit_buster_country.isquantityrestricted,
    _limit_buster_country.brand,
    _limit_buster_country.isfba,
    _limit_buster_country.upc,
    _limit_buster_country.upc_name,
    _limit_buster_country.msrp,
    _limit_buster_country.srp,
    _limit_buster_country.map,
    _limit_buster_country.parent_category,
    _limit_buster_country.child_category,
    _limit_buster_country.child_category_2,
    _limit_buster_country.child_category_3,
    _limit_buster_country.partner_name,
    _limit_buster_country.country_id,
    _limit_buster_country.offer_price,
    _limit_buster_country.offer_date_max_price,
    _limit_buster_country.units,
    _limit_buster_country.limit_buster_units,
    _limit_buster_country.redirect_asin,
    _limit_buster_country.created_date,
    _limit_buster_country.created_time,
    _limit_buster_country.sku_map,
    _limit_buster_country.sku_name,
    _limit_buster_country.buy_box_count,
    _limit_buster_country.partner_-_brand,
    _limit_buster_country.cod_condition,
    _limit_buster_country.unit_in_listing,
    _limit_buster_country.bp_brand_id,
    _limit_buster_country.asin_bp_enforceable,
    _limit_buster_country.country,
    _limit_buster_country.bbc_original_status,
    _limit_buster_country.bbc_2_8_22_status,
    _limit_buster_country.marketplace_channel_id,
    _limit_buster_country.marketplace_channel,
    _limit_buster_country.id,
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
    _Group_By.notes
  FROM _limit_buster_country
  LEFT JOIN _Group_By
    ON _limit_buster_country.brand = _Group_By.brand
    AND _limit_buster_country.sellerid = _Group_By.storefront_id
    AND _limit_buster_country.marketplace_channel_id = _Group_By.marketplace_channel_id
    AND _limit_buster_country.country_id = _Group_By.country_id
), _Join_Data_5 AS (
  SELECT
    _Group_by_UPC.upc,
    _Group_by_UPC.vendor_code,
    _Group_by_UPC.catalog_map,
    _Join_Data.brand,
    _Join_Data.storefront_id,
    _Join_Data.marketplace_channel_id,
    _Join_Data.marketplace_channel,
    _Join_Data.country_id,
    _Join_Data.country,
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
    _Join_Data.notes,
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
    _Join_Data.isfba,
    _Join_Data.upc_name,
    _Join_Data.msrp,
    _Join_Data.srp,
    _Join_Data.map,
    _Join_Data.parent_category,
    _Join_Data.child_category,
    _Join_Data.child_category_2,
    _Join_Data.child_category_3,
    _Join_Data.partner_name,
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
    _Join_Data.bbc_original_status,
    _Join_Data.bbc_2_8_22_status,
    _Join_Data.id
  FROM _Group_by_UPC
  RIGHT JOIN _Join_Data
    ON _Group_by_UPC.upc = _Join_Data.upc
), _Add_Formula_4 AS (
  SELECT
    upc,
    vendor_code,
    catalog_map,
    brand,
    storefront_id,
    marketplace_channel_id,
    marketplace_channel,
    country_id,
    country,
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
    notes,
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
    bbc_original_status,
    bbc_2_8_22_status,
    id,
    (
      CASE WHEN sellername = 'XXXXXXXXX' THEN CURRENT_DATE ELSE '' END
    ) AS enter_date,
    (
      CASE WHEN sellername = 'XXXXXXXXX' THEN CURRENT_DATE ELSE '' END
    ) AS new_notes,
    (
      CASE WHEN sellername = 'XXXXXXXXX' THEN CURRENT_DATE ELSE '' END
    ) AS new_invoice_id
  FROM _Join_Data_5
), _Add_Storefront_Enforceable AS (
  SELECT
    _Add_Formula_4.upc,
    _Add_Formula_4.vendor_code,
    _Add_Formula_4.catalog_map,
    _Add_Formula_4.brand,
    _Add_Formula_4.storefront_id,
    _Add_Formula_4.marketplace_channel_id,
    _Add_Formula_4.marketplace_channel,
    _Add_Formula_4.country_id,
    _Add_Formula_4.country,
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
    _Add_Formula_4.notes,
    _Add_Formula_4.sales_country_id,
    _Add_Formula_4.revenue,
    _Add_Formula_4.asins_units,
    _Add_Formula_4.revenue_brand,
    _Add_Formula_4.asins_units_brand,
    _Add_Formula_4.percent_mix_of_brand,
    _Add_Formula_4.revenue_rank_r3mth,
    _Add_Formula_4.revenue_distribution_r3mth,
    _Add_Formula_4.scraped_date,
    _Add_Formula_4.amazonid,
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
    _Add_Formula_4.bbc_original_status,
    _Add_Formula_4.bbc_2_8_22_status,
    _Add_Formula_4.id,
    _Add_Formula_4.enter_date,
    _Add_Formula_4.new_notes,
    _Add_Formula_4.new_invoice_id,
    _seller_brands_cols.enforcement_configuration_id,
    _seller_brands_cols.brand_assigned_user,
    _seller_brands_cols.brand_assigned_user_initials,
    _seller_brands_cols.storefront_enforceable,
    _seller_brands_cols.brand_enforceable,
    _seller_brands_cols.assigned_user_id
  FROM _Add_Formula_4
  LEFT JOIN _seller_brands_cols
    ON _Add_Formula_4.sellerid = _seller_brands_cols.seller_id
    AND _Add_Formula_4.bp_brand_id = _seller_brands_cols.bp_brand_id
    AND _Add_Formula_4.country_id = _seller_brands_cols.country_id
), _Filter_Rows AS (
  SELECT
    *
  FROM _Add_Storefront_Enforceable
  WHERE
    (
      brand_enforceable = 'Enforce'
      AND (
        storefront_enforceable = 'Enforce' AND country_id <= '2'
      )
    )
), _Full_Enforcement_Yesterday_DS AS (
  SELECT
    *
  FROM _Add_Storefront_Enforceable
), _Next_Enforcement_Beast_Mode AS (
  SELECT
    seller_id,
    enforcement_configuration_id,
    bp_brand_id,
    brand_assigned_user,
    brand_assigned_user_initials,
    storefront_enforceable,
    brand_enforceable,
    assigned_user_id,
    country_id,
    upc,
    vendor_code,
    catalog_map,
    brand,
    storefront_id,
    marketplace_channel_id,
    marketplace_channel,
    country,
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
    notes,
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
    asin_bp_enforceable,
    bbc_original_status,
    bbc_2_8_22_status,
    id,
    enter_date,
    new_notes,
    new_invoice_id,
    (
      COALESCE(units * offer_price, 0)
    ) AS inventory_value,
    (
      (
        offer_price - map
      ) / NULLIF(map, 0)
    ) AS price_delta_map,
    (
      (
        CASE
          WHEN brand = 'BioSil' AND last_notice_type = 'Certified Mail'
          THEN 'Investigate'
          WHEN brand = 'Pure Planet' AND last_notice_type = '2nd Email'
          THEN 'Investigate'
          WHEN brand IN ('Stokke', 'BABYZEN') AND last_notice_type = 'Certified Mail'
          THEN 'Investigate'
          WHEN (
            brand IN (
              'All American 1930',
              'de Buyer',
              'dpHUE',
              'BrainMD',
              'FEKKAI',
              'Masimo',
              'Stork',
              'Messermeister',
              'PetAg',
              'Scanpan',
              'TenPoint',
              'OOFOS',
              'Terry Naturally',
              'EuroMedica',
              'Milk Makeup',
              'Strider',
              'NEST New York',
              'North American Herb & Spice',
              'Kneipp',
              'Doona',
              'Mission',
              'Hydroxycut',
              'Yogasleep',
              'Bach'
            )
            OR partner_name IN ('Total Resources International')
          )
          AND last_notice_type IS NULL
          THEN CASE
            WHEN country = 'United States'
            THEN '1st Notice - Certified Mail'
            ELSE '1st Notice - USPS Mail'
          END
          WHEN brand IN ('Stokke', 'BABYZEN') AND last_notice_type = 'Initial Letter'
          THEN 'No Further Action'
          WHEN sellerid = 'A12SR9CZCS7BA0'
          THEN 'Contentious'
          WHEN last_notice_type IS NULL
          THEN '1st Notice - USPS Mail'
          WHEN last_notice_type = '1st Email'
          THEN '1st Notice - USPS Mail'
          WHEN last_notice_type = '2nd Email'
          THEN '1st Notice - USPS Mail'
          WHEN last_notice_type = '1st Notice - USPS Mail'
          THEN CASE
            WHEN country = 'United States'
            THEN '2nd Notice - Certified Mail'
            ELSE '2nd Notice - USPS Mail'
          END
          WHEN last_notice_type = '1st Notice - USPS Mail'
          AND delivery_status LIKE '%delivered%'
          THEN 'Certified Mail'
          WHEN last_notice_type = '1st Notice - Certified Mail'
          AND delivery_status LIKE '%delivered%'
          THEN CASE
            WHEN country = 'United States'
            THEN '2nd Notice - Certified Mail'
            ELSE '2nd Notice - USPS Mail'
          END
          WHEN last_notice_type IN (
            'Certified Mail',
            '1st Notice - Certified Mail',
            '2nd Notice - Certified Mail',
            '2nd Certified Mail'
          )
          AND delivery_status LIKE '%delivered%'
          THEN 'Cease & Desist'
          WHEN last_notice_type IN ('1st Notice - Certified Mail')
          AND delivery_status IN (
            'No Authorized Recipient Available',
            'Addressee Unknown',
            'Delivery Status Not Updated',
            'Arrived at Recipient PO',
            'Arrival at Unit',
            'Return Noticed Generated',
            'No Access',
            'Dead Mail/Disposed by Post Office',
            'Tendered to Agent for Final Delivery',
            'Redelivery Scheduled ',
            'Business Closed',
            'Arrival at Pickup Point',
            '2Nd Notice Generated',
            'Notice Left',
            'Pre-Shipment Info Sent to USPS',
            'Return to Sender',
            'Forwarded',
            'Return to Sender Processing',
            'No Secure Location Available',
            'Unclaimed',
            'Forward Expired',
            'Insufficient Address',
            'Dead Mail/Sent to Recovery Center',
            'Missent'
          )
          THEN 'Investigate / Resend'
          WHEN last_notice_type IN ('1st Notice - USPS Mail')
          AND delivery_status IN (
            'No Authorized Recipient Available',
            'Addressee Unknown',
            'Delivery Status Not Updated',
            'Arrived at Recipient PO',
            'Arrival at Unit',
            'Return Noticed Generated',
            'No Access',
            'Dead Mail/Disposed by Post Office',
            'Tendered to Agent for Final Delivery',
            'Redelivery Scheduled ',
            'Business Closed',
            'Arrival at Pickup Point',
            '2Nd Notice Generated',
            'Notice Left',
            'Pre-Shipment Info Sent to USPS',
            'Return to Sender',
            'Forwarded',
            'Return to Sender Processing',
            'No Secure Location Available',
            'Unclaimed',
            'Forward Expired',
            'Insufficient Address',
            'Dead Mail/Sent to Recovery Center',
            'Missent',
            'Enroute/Processed'
          )
          THEN 'Investigate / Resend 1st Notice - USPS Mail'
          WHEN last_notice_type IN (
            '1st Notice - Certified Mail',
            '2nd Notice - Certified Mail',
            'Certified Mail',
            '1st Notice - USPS Mail'
          )
          AND delivery_status IN (
            'En Route',
            'Shipment Acceptance',
            'Printed',
            'In Transit',
            'RESCHEDULED TO NEXT DELIVERY DAY    ',
            'Dispatched from Sort Facility ',
            'Enroute/Processed',
            'Out for Delivery',
            'Dispatched from Sort Facility',
            'AVAILABLE FOR REDLVRY OR PICKUP         ',
            'Provided to postal processing facility',
            'DLVRY EXCPT ANIMAL INTERFERENCE         ',
            '',
            'Provided to Postal Processing Facility',
            'Tendered to Agent for Final Delivery'
          )
          THEN 'Monitor'
          WHEN last_notice_type = 'USPS Mail'
          THEN 'Monitor'
          WHEN last_notice_type = 'Certified Mail' AND (
            delivery_status
          ) IS NULL
          THEN 'Investigate'
          WHEN last_notice_type = 'USPS Mail' AND c2m_error_status = 'FAILED'
          THEN 'Investigate'
          WHEN last_notice_type = 'Cease & Desist'
          THEN 'Investigate'
          WHEN last_notice_type = 'Misc Email'
          THEN 'Investigate'
          WHEN last_notice_type = '1st Email' AND last_note LIKE '%reply%'
          THEN '2nd Email Reply'
          WHEN storefront_name = 'iHerb LLC'
          THEN 'Tell Brand'
          WHEN storefront_name = 'OxKom'
          THEN 'Tell Brand'
          WHEN storefront_name = 'Swanson Health Products'
          THEN 'Tell Brand'
          WHEN partner_name = 'Nutraceutical'
          THEN 'Misc. Email'
          ELSE ''
        END
      )
    ) AS next_enforcement
  FROM _Filter_Rows
), _Group_By AS (
  SELECT
    scraped_date,
    brand,
    storefront_name,
    last_notice_type,
    next_enforcement,
    assigned_user_id,
    brand_assigned_user,
    last_notice_sent,
    sellerid,
    bp_brand_id,
    marketplace_channel,
    marketplace_channel_id,
    country_id,
    country,
    SUM(inventory_value) AS inventory_value,
    AVG(price_delta_map) AS price_deta_map,
    COUNT(asin) AS asin_count
  FROM _Next_Enforcement_Beast_Mode
  GROUP BY
    scraped_date,
    brand,
    storefront_name,
    last_notice_type,
    next_enforcement,
    assigned_user_id,
    brand_assigned_user,
    last_notice_sent,
    sellerid,
    bp_brand_id,
    marketplace_channel,
    marketplace_channel_id,
    country_id,
    country
), _Select_Columns AS (
  SELECT
    scraped_date,
    brand AS brand,
    storefront_name,
    last_notice_type AS last_notice_type,
    last_notice_sent AS last_notice_sent,
    next_enforcement,
    assigned_user_id,
    brand_assigned_user,
    inventory_value,
    price_deta_map AS avg_map_variance,
    sellerid AS storefront_id,
    asin_count,
    bp_brand_id,
    marketplace_channel,
    marketplace_channel_id,
    country_id,
    country
  FROM _Group_By
), _Enforcement_Actions_DS AS (
  SELECT
    *
  FROM _Select_Columns
)
SELECT
  *
FROM _Enforcement_Actions_DS