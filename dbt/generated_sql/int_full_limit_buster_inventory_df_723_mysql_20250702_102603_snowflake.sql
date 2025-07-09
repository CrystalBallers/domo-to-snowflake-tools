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
  SELECT
    id,
    asin,
    sellername,
    sellerid,
    quantity,
    isquantityrestricted,
    date,
    time,
    isfba,
    limitbuster,
    price,
    redirectedasin,
    site_extension,
    batch_id,
    batch_last_run
  FROM amazon_seller_inventory
), _Product_List_DataSet AS (
  SELECT
    amazon_seller_id,
    asin,
    brand_id,
    client_id,
    cost,
    deleted_at,
    fnsku,
    id,
    is_bundle,
    reorder_status,
    sku,
    units_in_listing,
    upc,
    vendor_parameter_id,
    vendor_sku,
    batch_id,
    batch_last_run,
    advertisement_enabled,
    description,
    estimated_annual_sales,
    member_since,
    partner_name,
    currency,
    estimated_anual_sales,
    legacy_brand_id,
    map_formula,
    brand,
    category,
    subcategory,
    brand_growth_target,
    msrp,
    map,
    amazon_asin_id,
    upc_id,
    country_id,
    srp,
    upc_srp,
    upc_default_cost,
    upc_default_discount,
    default_cost,
    default_discount,
    upc_map,
    wholesale_price,
    parent_asin,
    catalog_source,
    country_of_origin,
    min_order_qty,
    units_per_case,
    unit_lxwxh_inches,
    unit_weight_lb,
    sku_reorder_status,
    upc_reorder_status,
    is_discontinued_status,
    upc_components,
    catalog_last_appearance,
    parent_category,
    child_category,
    child_category_3,
    child_category_2,
    sku_reorder_comments,
    upc_reorder_comments,
    is_preferred_sku_by_asin,
    seller_name,
    countries_name,
    sku_created_date,
    upc_created_date,
    sku_cost,
    is_current,
    fulfillment_channel,
    last_seen_price,
    last_known_price,
    estimated_units_sold_last_month,
    hero,
    upc_count_total_components,
    brand_created_at,
    seller_start_date,
    brand_start_date,
    amazon_seller_central_accounts_created_at,
    bp_partner_name,
    bp_brand_name,
    partner_-_brand,
    seller_created_at,
    preferred_skus_on_asin,
    qa_-_count_fbm_offers_current,
    qa_-_count_fba_offers_current,
    qa_-count_reorder_status_yes,
    qa_-count_preferred_skus,
    qa_-current_preferred_skus,
    is_current_asin,
    name,
    upc_name
  FROM Product_List_DataSet
), _View_of_UPCs_w_Categories_v2 AS (
  SELECT
    brand_id,
    cases_per_layer,
    cases_per_pallet,
    loreal_media_pillar_goals,
    catalog_source,
    loreal_media_categories,
    client_id,
    loreal_class_media,
    cost,
    loreal_franchise,
    created_at,
    loreal_sub_franchise,
    created_by,
    loreal_division,
    default_cost,
    loreal_axe_media,
    default_discount,
    loreal_sub_axe_media,
    deleted_at,
    loreal_hero_cmo_mapping,
    deleted_by,
    loreal_hero_parent,
    id,
    loreal_hero,
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
    parent_category,
    child_category,
    child_category_2,
    child_category_3,
    batch_id,
    batch_last_run
  FROM View_of_UPCs_w_Categories_v2
), _bp_brands_mapping AS (
  SELECT
    brand_id,
    client_id,
    bp_partner_id,
    bp_brand_id,
    batch_id,
    batch_last_run
  FROM bp_brands_mapping
), _bp_asin_enforcement_settings AS (
  SELECT
    source,
    country_id,
    country_name,
    asin,
    units_in_listing,
    is_bp_enforceable,
    offers_scraper_enabled,
    suppression_tracking_enabled,
    is_hero,
    hero_configuration,
    transparency_status,
    asin_title,
    cpt,
    brand_name,
    client_name,
    buy_box_status,
    buy_box_status_clasification,
    is_minderest_tracking,
    asin_active,
    map,
    last_seen_price,
    upc,
    is_bundle,
    brand_assigned_user,
    brand_assigned_user_initials,
    recom_price,
    last_scraper_seen_date,
    asin_scraped_offers_count,
    ltm_rev,
    asin_l90_avg_sales,
    asin_l90_active_bb_avg_sales,
    batch_id,
    batch_last_run
  FROM bp_asin_enforcement_settings
), _Offers_Storefronts_by_Date AS (
  SELECT
    scraped_date,
    asin,
    amazonid,
    storefront_name,
    country_id,
    isfba,
    isprime,
    isamazon,
    cod_condition,
    offer_date_max_price,
    offer_date_min_price,
    offer_date_avg_price,
    offer_date_count,
    buy_box_count,
    batch_id,
    batch_last_run
  FROM Offers_Storefronts_by_Date
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
), _BP_Gated_Status AS (
  SELECT
    asin,
    upc,
    catalog_brand,
    bbc_original_status,
    bbc_2_8_22_status
  FROM BP_Gated_Status
), _ASIN_Rolling_3_Month_Revenue_Rank_DPDS AS (
  SELECT
    brand,
    partner_name,
    asin,
    sales_country_id,
    revenue,
    asins_units,
    revenue_brand,
    asins_units_brand,
    percent_mix_of_brand,
    revenue_rank_r3mth,
    revenue_distribution_r3mth
  FROM ASIN_Rolling_3_Month_Revenue_Rank_DPDS
), _Change_price_typ_to_text AS (
  SELECT
    CAST(price AS VARCHAR),
    id,
    asin,
    sellername,
    sellerid,
    quantity,
    isquantityrestricted,
    date,
    time,
    isfba,
    limitbuster,
    redirectedasin,
    site_extension,
    batch_id,
    batch_last_run
  FROM _amazon_seller_inventory
), _brands_mapping AS (
  SELECT
    _Product_List_DataSet.amazon_seller_id,
    _Product_List_DataSet.asin,
    _Product_List_DataSet.brand_id,
    _Product_List_DataSet.client_id,
    _Product_List_DataSet.cost,
    _Product_List_DataSet.deleted_at,
    _Product_List_DataSet.fnsku,
    _Product_List_DataSet.id,
    _Product_List_DataSet.is_bundle,
    _Product_List_DataSet.reorder_status,
    _Product_List_DataSet.sku,
    _Product_List_DataSet.units_in_listing,
    _Product_List_DataSet.upc,
    _Product_List_DataSet.vendor_parameter_id,
    _Product_List_DataSet.vendor_sku,
    _Product_List_DataSet.batch_id,
    _Product_List_DataSet.batch_last_run,
    _Product_List_DataSet.advertisement_enabled,
    _Product_List_DataSet.description,
    _Product_List_DataSet.estimated_annual_sales,
    _Product_List_DataSet.member_since,
    _Product_List_DataSet.partner_name,
    _Product_List_DataSet.currency,
    _Product_List_DataSet.estimated_anual_sales,
    _Product_List_DataSet.legacy_brand_id,
    _Product_List_DataSet.map_formula,
    _Product_List_DataSet.brand,
    _Product_List_DataSet.category,
    _Product_List_DataSet.subcategory,
    _Product_List_DataSet.brand_growth_target,
    _Product_List_DataSet.msrp,
    _Product_List_DataSet.map,
    _Product_List_DataSet.amazon_asin_id,
    _Product_List_DataSet.upc_id,
    _Product_List_DataSet.country_id,
    _Product_List_DataSet.srp,
    _Product_List_DataSet.upc_srp,
    _Product_List_DataSet.upc_default_cost,
    _Product_List_DataSet.upc_default_discount,
    _Product_List_DataSet.default_cost,
    _Product_List_DataSet.default_discount,
    _Product_List_DataSet.upc_map,
    _Product_List_DataSet.wholesale_price,
    _Product_List_DataSet.parent_asin,
    _Product_List_DataSet.catalog_source,
    _Product_List_DataSet.country_of_origin,
    _Product_List_DataSet.min_order_qty,
    _Product_List_DataSet.units_per_case,
    _Product_List_DataSet.unit_lxwxh_inches,
    _Product_List_DataSet.unit_weight_lb,
    _Product_List_DataSet.sku_reorder_status,
    _Product_List_DataSet.upc_reorder_status,
    _Product_List_DataSet.is_discontinued_status,
    _Product_List_DataSet.upc_components,
    _Product_List_DataSet.catalog_last_appearance,
    _Product_List_DataSet.parent_category,
    _Product_List_DataSet.child_category,
    _Product_List_DataSet.child_category_3,
    _Product_List_DataSet.child_category_2,
    _Product_List_DataSet.sku_reorder_comments,
    _Product_List_DataSet.upc_reorder_comments,
    _Product_List_DataSet.is_preferred_sku_by_asin,
    _Product_List_DataSet.seller_name,
    _Product_List_DataSet.countries_name,
    _Product_List_DataSet.sku_created_date,
    _Product_List_DataSet.upc_created_date,
    _Product_List_DataSet.sku_cost,
    _Product_List_DataSet.is_current,
    _Product_List_DataSet.fulfillment_channel,
    _Product_List_DataSet.last_seen_price,
    _Product_List_DataSet.last_known_price,
    _Product_List_DataSet.estimated_units_sold_last_month,
    _Product_List_DataSet.hero,
    _Product_List_DataSet.upc_count_total_components,
    _Product_List_DataSet.brand_created_at,
    _Product_List_DataSet.seller_start_date,
    _Product_List_DataSet.brand_start_date,
    _Product_List_DataSet.amazon_seller_central_accounts_created_at,
    _Product_List_DataSet.bp_partner_name,
    _Product_List_DataSet.bp_brand_name,
    _Product_List_DataSet.partner_-_brand,
    _Product_List_DataSet.seller_created_at,
    _Product_List_DataSet.preferred_skus_on_asin,
    _Product_List_DataSet.qa_-_count_fbm_offers_current,
    _Product_List_DataSet.qa_-_count_fba_offers_current,
    _Product_List_DataSet.qa_-count_reorder_status_yes,
    _Product_List_DataSet.qa_-count_preferred_skus,
    _Product_List_DataSet.qa_-current_preferred_skus,
    _Product_List_DataSet.is_current_asin,
    _Product_List_DataSet.name,
    _Product_List_DataSet.upc_name,
    _bp_brands_mapping.bp_partner_id,
    _bp_brands_mapping.bp_brand_id
  FROM _Product_List_DataSet
  INNER JOIN _bp_brands_mapping
    ON _Product_List_DataSet.brand_id = _bp_brands_mapping.brand_id
    AND _Product_List_DataSet.client_id = _bp_brands_mapping.client_id
), _Select_Columns AS (
  SELECT
    asin,
    is_bp_enforceable AS asin_bp_enforceable
  FROM _bp_asin_enforcement_settings
), _select_name AS (
  SELECT
    id,
    name AS country
  FROM _countries
), _Select_Gated_Columns AS (
  SELECT
    asin,
    bbc_original_status,
    bbc_2_8_22_status
  FROM _BP_Gated_Status
), _Modify_date_time_storefront_name_and_price AS (
  SELECT
    id,
    asin,
    sellername,
    sellerid,
    quantity,
    isquantityrestricted,
    date,
    time,
    isfba,
    limitbuster,
    redirectedasin,
    site_extension,
    batch_id,
    batch_last_run,
    (
      date
    ) AS created_date,
    (
      time
    ) AS created_time,
    (
      TRIM(sellername)
    ) AS storefront_name,
    (
      CASE
        WHEN STR_CONTAINS(price, '$')
        THEN SUBSTRING(price, 1, (
          FLOOR(LENGTH(price)) / NULLIF(2, 0)
        ) + 1)
        ELSE price
      END
    ) AS price
  FROM _Change_price_typ_to_text
), _Group_By AS (
  SELECT
    asin,
    brand_id,
    brand,
    upc_name,
    msrp,
    map,
    upc,
    partner_name,
    upc_id,
    country_id,
    amazon_seller_id,
    partner_-_brand,
    bp_brand_id,
    COUNT(sku) AS total_products,
    MIN(name) AS sku_name,
    SUM(units_in_listing) AS unit_in_listing
  FROM _brands_mapping
  GROUP BY
    asin,
    brand_id,
    brand,
    upc_name,
    msrp,
    map,
    upc,
    partner_name,
    upc_id,
    country_id,
    amazon_seller_id,
    partner_-_brand,
    bp_brand_id
), _Join_Data_3 AS (
  SELECT
    _Offers_Storefronts_by_Date.scraped_date,
    _Offers_Storefronts_by_Date.asin,
    _Offers_Storefronts_by_Date.amazonid,
    _Offers_Storefronts_by_Date.storefront_name,
    _Offers_Storefronts_by_Date.country_id,
    _Offers_Storefronts_by_Date.isfba,
    _Offers_Storefronts_by_Date.isprime,
    _Offers_Storefronts_by_Date.isamazon,
    _Offers_Storefronts_by_Date.cod_condition,
    _Offers_Storefronts_by_Date.offer_date_max_price,
    _Offers_Storefronts_by_Date.offer_date_min_price,
    _Offers_Storefronts_by_Date.offer_date_avg_price,
    _Offers_Storefronts_by_Date.offer_date_count,
    _Offers_Storefronts_by_Date.buy_box_count,
    _Offers_Storefronts_by_Date.batch_id,
    _Offers_Storefronts_by_Date.batch_last_run,
    _select_name.country
  FROM _Offers_Storefronts_by_Date
  INNER JOIN _select_name
    ON _Offers_Storefronts_by_Date.country_id = _select_name.id
), _Add_decimal_to_price AS (
  SELECT
    CAST(price AS DECIMAL(38, 0)),
    id,
    asin,
    sellername,
    sellerid,
    quantity,
    isquantityrestricted,
    date,
    time,
    isfba,
    limitbuster,
    redirectedasin,
    site_extension,
    batch_id,
    batch_last_run,
    created_date,
    created_time,
    storefront_name
  FROM _Modify_date_time_storefront_name_and_price
), _Join_Data_2 AS (
  SELECT
    _Group_By.asin,
    _Group_By.brand_id,
    _Group_By.brand,
    _Group_By.upc_name,
    _Group_By.msrp,
    _Group_By.map,
    _Group_By.upc,
    _Group_By.partner_name,
    _Group_By.upc_id,
    _Group_By.country_id,
    _Group_By.amazon_seller_id,
    _Group_By.partner_-_brand,
    _Group_By.bp_brand_id,
    _Group_By.total_products,
    _Group_By.sku_name,
    _Group_By.unit_in_listing,
    _Select_Columns.asin_bp_enforceable
  FROM _Group_By
  LEFT JOIN _Select_Columns
    ON _Group_By.asin = _Select_Columns.asin
), _Remove_spaces_from_storefront_name AS (
  SELECT
    scraped_date,
    asin,
    amazonid,
    country_id,
    isfba,
    isprime,
    isamazon,
    cod_condition,
    offer_date_max_price,
    offer_date_min_price,
    offer_date_avg_price,
    offer_date_count,
    buy_box_count,
    batch_id,
    batch_last_run,
    country,
    (
      TRIM(storefront_name)
    ) AS storefront_name
  FROM _Join_Data_3
), _Remove_unnecessary_columns AS (
  SELECT
    sellername,
    storefront_name,
    sellerid,
    asin,
    isquantityrestricted,
    isfba,
    quantity AS units,
    limitbuster AS limit_buster_units,
    created_time,
    created_date,
    price AS offer_price,
    redirectedasin AS redirect_asin,
    site_extension
  FROM _Add_decimal_to_price
), _ASIN_UPC AS (
  SELECT
    _Join_Data_2.asin,
    _Join_Data_2.brand_id,
    _Join_Data_2.brand,
    _Join_Data_2.upc_name,
    _Join_Data_2.msrp,
    _Join_Data_2.map AS sku_map,
    _Join_Data_2.upc,
    _Join_Data_2.partner_name,
    _Join_Data_2.upc_id,
    _Join_Data_2.country_id,
    _Join_Data_2.amazon_seller_id,
    _Join_Data_2.partner_-_brand,
    _Join_Data_2.bp_brand_id,
    _Join_Data_2.total_products,
    _Join_Data_2.sku_name,
    _Join_Data_2.unit_in_listing,
    _Join_Data_2.asin_bp_enforceable,
    _View_of_UPCs_w_Categories_v2.brand_id AS upcs_w_categories_brand_id,
    _View_of_UPCs_w_Categories_v2.cases_per_layer,
    _View_of_UPCs_w_Categories_v2.cases_per_pallet,
    _View_of_UPCs_w_Categories_v2.loreal_media_pillar_goals,
    _View_of_UPCs_w_Categories_v2.catalog_source,
    _View_of_UPCs_w_Categories_v2.loreal_media_categories,
    _View_of_UPCs_w_Categories_v2.client_id,
    _View_of_UPCs_w_Categories_v2.loreal_class_media,
    _View_of_UPCs_w_Categories_v2.cost,
    _View_of_UPCs_w_Categories_v2.loreal_franchise,
    _View_of_UPCs_w_Categories_v2.created_at,
    _View_of_UPCs_w_Categories_v2.loreal_sub_franchise,
    _View_of_UPCs_w_Categories_v2.created_by,
    _View_of_UPCs_w_Categories_v2.loreal_division,
    _View_of_UPCs_w_Categories_v2.default_cost,
    _View_of_UPCs_w_Categories_v2.loreal_axe_media,
    _View_of_UPCs_w_Categories_v2.default_discount,
    _View_of_UPCs_w_Categories_v2.loreal_sub_axe_media,
    _View_of_UPCs_w_Categories_v2.deleted_at,
    _View_of_UPCs_w_Categories_v2.loreal_hero_cmo_mapping,
    _View_of_UPCs_w_Categories_v2.deleted_by,
    _View_of_UPCs_w_Categories_v2.loreal_hero_parent,
    _View_of_UPCs_w_Categories_v2.id,
    _View_of_UPCs_w_Categories_v2.loreal_hero,
    _View_of_UPCs_w_Categories_v2.hero,
    _View_of_UPCs_w_Categories_v2.map,
    _View_of_UPCs_w_Categories_v2.min_order_qty,
    _View_of_UPCs_w_Categories_v2.name,
    _View_of_UPCs_w_Categories_v2.old_upc,
    _View_of_UPCs_w_Categories_v2.parent_upc,
    _View_of_UPCs_w_Categories_v2.reorder_comments,
    _View_of_UPCs_w_Categories_v2.reorder_status,
    _View_of_UPCs_w_Categories_v2.srp,
    _View_of_UPCs_w_Categories_v2.unit_lxwxh_inches,
    _View_of_UPCs_w_Categories_v2.unit_weight_lb,
    _View_of_UPCs_w_Categories_v2.units_per_case,
    _View_of_UPCs_w_Categories_v2.updated_at,
    _View_of_UPCs_w_Categories_v2.updated_by,
    _View_of_UPCs_w_Categories_v2.vendor_sku,
    _View_of_UPCs_w_Categories_v2.wholesale_price,
    _View_of_UPCs_w_Categories_v2.is_discontinued_status,
    _View_of_UPCs_w_Categories_v2.unit_count_type_id,
    _View_of_UPCs_w_Categories_v2.unit_count,
    _View_of_UPCs_w_Categories_v2.parent_category,
    _View_of_UPCs_w_Categories_v2.child_category,
    _View_of_UPCs_w_Categories_v2.child_category_2,
    _View_of_UPCs_w_Categories_v2.child_category_3,
    _View_of_UPCs_w_Categories_v2.batch_id,
    _View_of_UPCs_w_Categories_v2.batch_last_run
  FROM _Join_Data_2
  LEFT JOIN _View_of_UPCs_w_Categories_v2
    ON _Join_Data_2.upc = _View_of_UPCs_w_Categories_v2.upc
    AND _Join_Data_2.upc_id = _View_of_UPCs_w_Categories_v2.id
    AND _Join_Data_2.amazon_seller_id = _View_of_UPCs_w_Categories_v2.amazon_seller_id
), _Remove_TODAY_from_Inventory AS (
  SELECT
    *
  FROM _Remove_unnecessary_columns
  WHERE
    created_date < NULL
), _Country_ID AS (
  SELECT
    sellername,
    storefront_name,
    sellerid,
    asin,
    isquantityrestricted,
    isfba,
    units,
    limit_buster_units,
    created_time,
    created_date,
    offer_price,
    redirect_asin,
    site_extension,
    (
      CASE
        WHEN site_extension = 'com'
        THEN 1
        WHEN site_extension = 'ca'
        THEN 2
        WHEN site_extension = 'co.uk'
        THEN 3
        WHEN site_extension = 'fr'
        THEN 4
        WHEN site_extension = 'it'
        THEN 5
        WHEN site_extension = 'de'
        THEN 6
        WHEN site_extension = 'es'
        THEN 7
        WHEN site_extension = 'nl'
        THEN 8
        WHEN site_extension = 'pl'
        THEN 9
        WHEN site_extension = 'se'
        THEN 10
        WHEN site_extension = 'be'
        THEN 11
        ELSE 0
      END
    ) AS country_id
  FROM _Remove_TODAY_from_Inventory
), _Join_with_all_scraped_storefronts AS (
  SELECT
    _Remove_spaces_from_storefront_name.scraped_date,
    _Remove_spaces_from_storefront_name.asin,
    _Remove_spaces_from_storefront_name.amazonid,
    _Remove_spaces_from_storefront_name.storefront_name,
    _Remove_spaces_from_storefront_name.country_id,
    _Remove_spaces_from_storefront_name.isfba,
    _Remove_spaces_from_storefront_name.isprime,
    _Remove_spaces_from_storefront_name.isamazon,
    _Remove_spaces_from_storefront_name.cod_condition,
    _Remove_spaces_from_storefront_name.offer_date_max_price,
    _Remove_spaces_from_storefront_name.buy_box_count,
    _Remove_spaces_from_storefront_name.batch_id,
    _Remove_spaces_from_storefront_name.batch_last_run,
    _Remove_spaces_from_storefront_name.country,
    _Country_ID.sellername,
    _Country_ID.sellerid,
    _Country_ID.isquantityrestricted,
    _Country_ID.units,
    _Country_ID.limit_buster_units,
    _Country_ID.created_time,
    _Country_ID.created_date,
    _Country_ID.offer_price,
    _Country_ID.redirect_asin,
    _Country_ID.site_extension,
    _Country_ID.country_id
  FROM _Remove_spaces_from_storefront_name
  LEFT JOIN _Country_ID
    ON _Remove_spaces_from_storefront_name.asin = _Country_ID.asin
    AND _Remove_spaces_from_storefront_name.scraped_date = _Country_ID.created_date
    AND _Remove_spaces_from_storefront_name.storefront_name = _Country_ID.storefront_name
    AND _Remove_spaces_from_storefront_name.isfba = _Country_ID.isfba
    AND _Remove_spaces_from_storefront_name.country_id = _Country_ID.country_id
), _Join_Data AS (
  SELECT
    _Join_with_all_scraped_storefronts.sellername,
    _Join_with_all_scraped_storefronts.storefront_name,
    _Join_with_all_scraped_storefronts.sellerid,
    _Join_with_all_scraped_storefronts.asin,
    _Join_with_all_scraped_storefronts.isquantityrestricted,
    _Join_with_all_scraped_storefronts.isfba,
    _Join_with_all_scraped_storefronts.units,
    _Join_with_all_scraped_storefronts.limit_buster_units,
    _Join_with_all_scraped_storefronts.created_time,
    _Join_with_all_scraped_storefronts.created_date,
    _Join_with_all_scraped_storefronts.offer_price,
    _Join_with_all_scraped_storefronts.redirect_asin,
    _Join_with_all_scraped_storefronts.site_extension,
    _Join_with_all_scraped_storefronts.country_id,
    _Join_with_all_scraped_storefronts.scraped_date,
    _Join_with_all_scraped_storefronts.amazonid,
    _Join_with_all_scraped_storefronts.country_id,
    _Join_with_all_scraped_storefronts.isfba,
    _Join_with_all_scraped_storefronts.isprime,
    _Join_with_all_scraped_storefronts.isamazon,
    _Join_with_all_scraped_storefronts.cod_condition,
    _Join_with_all_scraped_storefronts.offer_date_max_price,
    _Join_with_all_scraped_storefronts.offer_date_min_price,
    _Join_with_all_scraped_storefronts.offer_date_avg_price,
    _Join_with_all_scraped_storefronts.offer_date_count,
    _Join_with_all_scraped_storefronts.buy_box_count,
    _Join_with_all_scraped_storefronts.batch_id,
    _Join_with_all_scraped_storefronts.batch_last_run,
    _Join_with_all_scraped_storefronts.country,
    _ASIN_UPC.brand_id,
    _ASIN_UPC.cases_per_layer,
    _ASIN_UPC.cases_per_pallet,
    _ASIN_UPC.loreal_media_pillar_goals,
    _ASIN_UPC.catalog_source,
    _ASIN_UPC.loreal_media_categories,
    _ASIN_UPC.client_id,
    _ASIN_UPC.loreal_class_media,
    _ASIN_UPC.cost,
    _ASIN_UPC.loreal_franchise,
    _ASIN_UPC.created_at,
    _ASIN_UPC.loreal_sub_franchise,
    _ASIN_UPC.created_by,
    _ASIN_UPC.loreal_division,
    _ASIN_UPC.default_cost,
    _ASIN_UPC.loreal_axe_media,
    _ASIN_UPC.default_discount,
    _ASIN_UPC.loreal_sub_axe_media,
    _ASIN_UPC.deleted_at,
    _ASIN_UPC.loreal_hero_cmo_mapping,
    _ASIN_UPC.deleted_by,
    _ASIN_UPC.loreal_hero_parent,
    _ASIN_UPC.id,
    _ASIN_UPC.loreal_hero,
    _ASIN_UPC.hero,
    _ASIN_UPC.sku_map,
    _ASIN_UPC.min_order_qty,
    _ASIN_UPC.name,
    _ASIN_UPC.old_upc,
    _ASIN_UPC.parent_upc,
    _ASIN_UPC.reorder_comments,
    _ASIN_UPC.reorder_status,
    _ASIN_UPC.srp,
    _ASIN_UPC.unit_lxwxh_inches,
    _ASIN_UPC.unit_weight_lb,
    _ASIN_UPC.units_per_case,
    _ASIN_UPC.upc,
    _ASIN_UPC.updated_at,
    _ASIN_UPC.updated_by,
    _ASIN_UPC.vendor_sku,
    _ASIN_UPC.wholesale_price,
    _ASIN_UPC.is_discontinued_status,
    _ASIN_UPC.amazon_seller_id,
    _ASIN_UPC.unit_count_type_id,
    _ASIN_UPC.unit_count,
    _ASIN_UPC.parent_category,
    _ASIN_UPC.child_category,
    _ASIN_UPC.child_category_2,
    _ASIN_UPC.child_category_3,
    _ASIN_UPC.upcs_w_categories_brand_id,
    _ASIN_UPC.brand,
    _ASIN_UPC.upc_name,
    _ASIN_UPC.msrp,
    _ASIN_UPC.map,
    _ASIN_UPC.partner_name,
    _ASIN_UPC.upc_id,
    _ASIN_UPC.partner_-_brand,
    _ASIN_UPC.bp_brand_id,
    _ASIN_UPC.total_products,
    _ASIN_UPC.sku_name,
    _ASIN_UPC.unit_in_listing,
    _ASIN_UPC.asin_bp_enforceable
  FROM _Join_with_all_scraped_storefronts
  LEFT JOIN _ASIN_UPC
    ON _Join_with_all_scraped_storefronts.asin = _ASIN_UPC.asin
    AND _Join_with_all_scraped_storefronts.country_id = _ASIN_UPC.country_id
), _Removed_unnecessary_columns AS (
  SELECT
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
    country
  FROM _Join_Data
), _Modify_amazonId_sellerId_storefront_name_and_sellerName AS (
  SELECT
    scraped_date,
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
    (
      COALESCE(amazonid, sellerid)
    ) AS amazonid,
    (
      COALESCE(sellerid, amazonid)
    ) AS sellerid,
    (
      TRIM(storefront_name)
    ) AS storefront_name,
    (
      TRIM(sellername)
    ) AS sellername
  FROM _Removed_unnecessary_columns
), _Remove_Duplicated_ASINs AS (
  SELECT DISTINCT
    *
  FROM _Modify_amazonId_sellerId_storefront_name_and_sellerName
), _change_columns_if_they_are_null AS (
  SELECT
    scraped_date,
    amazonid,
    sellerid,
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
    offer_date_max_price,
    units,
    limit_buster_units,
    redirect_asin,
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
    (
      CASE WHEN offer_price IS NULL THEN offer_date_max_price ELSE offer_price END
    ) AS offer_price,
    (
      CASE WHEN created_date IS NULL THEN scraped_date ELSE created_date END
    ) AS created_date,
    (
      CASE WHEN sellername IS NULL THEN storefront_name ELSE sellername END
    ) AS sellername
  FROM _Remove_Duplicated_ASINs
), _Join_Gated_Status AS (
  SELECT
    _change_columns_if_they_are_null.scraped_date,
    _change_columns_if_they_are_null.amazonid,
    _change_columns_if_they_are_null.sellerid,
    _change_columns_if_they_are_null.sellername,
    _change_columns_if_they_are_null.storefront_name,
    _change_columns_if_they_are_null.asin,
    _change_columns_if_they_are_null.isquantityrestricted,
    _change_columns_if_they_are_null.brand,
    _change_columns_if_they_are_null.isfba,
    _change_columns_if_they_are_null.upc,
    _change_columns_if_they_are_null.upc_name,
    _change_columns_if_they_are_null.msrp,
    _change_columns_if_they_are_null.srp,
    _change_columns_if_they_are_null.map,
    _change_columns_if_they_are_null.parent_category,
    _change_columns_if_they_are_null.child_category,
    _change_columns_if_they_are_null.child_category_2,
    _change_columns_if_they_are_null.child_category_3,
    _change_columns_if_they_are_null.partner_name,
    _change_columns_if_they_are_null.country_id,
    _change_columns_if_they_are_null.offer_price,
    _change_columns_if_they_are_null.offer_date_max_price,
    _change_columns_if_they_are_null.units,
    _change_columns_if_they_are_null.limit_buster_units,
    _change_columns_if_they_are_null.redirect_asin,
    _change_columns_if_they_are_null.created_date,
    _change_columns_if_they_are_null.created_time,
    _change_columns_if_they_are_null.sku_map,
    _change_columns_if_they_are_null.sku_name,
    _change_columns_if_they_are_null.buy_box_count,
    _change_columns_if_they_are_null.partner_-_brand,
    _change_columns_if_they_are_null.cod_condition,
    _change_columns_if_they_are_null.unit_in_listing,
    _change_columns_if_they_are_null.bp_brand_id,
    _change_columns_if_they_are_null.asin_bp_enforceable,
    _change_columns_if_they_are_null.country,
    _Select_Gated_Columns.bbc_original_status,
    _Select_Gated_Columns.bbc_2_8_22_status
  FROM _change_columns_if_they_are_null
  LEFT JOIN _Select_Gated_Columns
    ON _change_columns_if_they_are_null.asin = _Select_Gated_Columns.asin
), _Join_Data_1 AS (
  SELECT
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.sales_country_id,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.revenue,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.asins_units,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.revenue_brand,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.asins_units_brand,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.percent_mix_of_brand,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.revenue_rank_r3mth,
    _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.revenue_distribution_r3mth,
    _Join_Gated_Status.asin,
    _Join_Gated_Status.bbc_original_status,
    _Join_Gated_Status.bbc_2_8_22_status,
    _Join_Gated_Status.scraped_date,
    _Join_Gated_Status.amazonid,
    _Join_Gated_Status.sellerid,
    _Join_Gated_Status.sellername,
    _Join_Gated_Status.storefront_name,
    _Join_Gated_Status.asin,
    _Join_Gated_Status.isquantityrestricted,
    _Join_Gated_Status.brand,
    _Join_Gated_Status.isfba,
    _Join_Gated_Status.upc,
    _Join_Gated_Status.upc_name,
    _Join_Gated_Status.msrp,
    _Join_Gated_Status.srp,
    _Join_Gated_Status.map,
    _Join_Gated_Status.parent_category,
    _Join_Gated_Status.child_category,
    _Join_Gated_Status.child_category_2,
    _Join_Gated_Status.child_category_3,
    _Join_Gated_Status.partner_name,
    _Join_Gated_Status.country_id,
    _Join_Gated_Status.offer_price,
    _Join_Gated_Status.offer_date_max_price,
    _Join_Gated_Status.units,
    _Join_Gated_Status.limit_buster_units,
    _Join_Gated_Status.redirect_asin,
    _Join_Gated_Status.created_date,
    _Join_Gated_Status.created_time,
    _Join_Gated_Status.sku_map,
    _Join_Gated_Status.sku_name,
    _Join_Gated_Status.buy_box_count,
    _Join_Gated_Status.partner_-_brand,
    _Join_Gated_Status.cod_condition,
    _Join_Gated_Status.unit_in_listing,
    _Join_Gated_Status.bp_brand_id,
    _Join_Gated_Status.asin_bp_enforceable,
    _Join_Gated_Status.country
  FROM _ASIN_Rolling_3_Month_Revenue_Rank_DPDS
  RIGHT JOIN _Join_Gated_Status
    ON _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.asin = _Join_Gated_Status.asin
    AND _ASIN_Rolling_3_Month_Revenue_Rank_DPDS.sales_country_id = _Join_Gated_Status.country_id
), _Value_Mapper AS (
  SELECT
    sales_country_id,
    revenue,
    asins_units,
    revenue_brand,
    asins_units_brand,
    percent_mix_of_brand,
    CASE WHEN revenue_rank_r3mth IS NULL THEN '-1' ELSE revenue_rank_r3mth END AS revenue_rank_r3mth,
    revenue_distribution_r3mth,
    asin,
    bbc_original_status,
    bbc_2_8_22_status,
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
    country
  FROM _Join_Data_1
), _Filter_Yesterday AS (
  SELECT
    *
  FROM _Value_Mapper
  WHERE
    scraped_date = DATEADD(DAY, '1' * -1, CURRENT_DATE)
), _Full_Limit_Buster_Inventory_DS AS (
  SELECT
    *
  FROM _Filter_Yesterday
), _Full_Limit_Buster_Inventory_DS_Yesterday AS (
  SELECT
    *
  FROM _Filter_Yesterday
)
SELECT
  *
FROM _Full_Limit_Buster_Inventory_DS_Yesterday