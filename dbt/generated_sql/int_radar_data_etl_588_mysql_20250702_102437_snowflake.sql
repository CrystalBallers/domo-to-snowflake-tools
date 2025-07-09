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
  SELECT
    brand_id,
    client_id,
    cogs,
    country_id,
    purchase_date,
    expanded_units,
    id,
    product_id,
    sales,
    units,
    brand_name,
    client_name,
    amazon_seller_id,
    country_1,
    country_name,
    country_short_name,
    country_sales_channel,
    country_currency,
    currency_value,
    currency_symbol,
    item_price_local,
    brand_name,
    partner_name,
    asin,
    sku,
    upc,
    upc_name,
    upc_reorder_status,
    units_in_listing,
    currency,
    parent_category,
    child_category,
    child_category_3,
    child_category_2,
    is_bundle,
    asin_name,
    parent_asin,
    pack_size,
    srp,
    msrp,
    brand_created_at,
    brand_start_date,
    seller_start_date,
    seller_created_at,
    account_name,
    date,
    ad_impressions,
    ad_clicks,
    ad_units,
    ad_orders,
    ad_spend,
    ad_revenue,
    to_join_asin_performance,
    no_inventory_days,
    fba_fulfillable_doh,
    inbound_at_amazon_doh,
    inbound_in_transit_doh,
    rc_doh,
    on_order_doh,
    buy_box_percentage,
    weighted_average_bbp,
    traff_page_views,
    traff_sessions,
    cvr,
    traff_page_views_browser,
    traff_page_views_mobile,
    traff_sessions_browser,
    traff_sessions_mobile,
    traff_units_ordered,
    traff_units_ordered_b2b,
    traff_units_ordered_total,
    dm_current_date,
    dm_relative_date,
    dm_yesterday_flag,
    dm_sdlw_flag,
    dm_sdow_flag,
    dm_last_07_flag,
    dm_prior_07_flag,
    dm_last_28_flag,
    dm_year,
    dm_ym,
    dm_month,
    dm_month_full_name,
    dm_month_short_name,
    dm_today_flag,
    dm_week_sun,
    dm_week_mon,
    dm_yr_wk,
    dm_pr_yr_wk,
    dm_prior_day_flag,
    dm_current_month_flag,
    dm_last_month_flag,
    dm_prior_month_flag,
    dm_last_year_month_flag,
    dm_prior_year_month_flag,
    dm_ytd_flag,
    dm_last_ytd_flag,
    dm_mtd_flag,
    dm_last_mtd_flag,
    dm_last_year_mtd_flag,
    dm_domo_current_date,
    dm_domo_current_time,
    dm_domo_current_timestamp,
    dm_month_int,
    dm_current_year_flag,
    dm_last_year_flag,
    dm_last_12m_flag,
    dm_prior_12m_flag,
    dm_day_of_year,
    dm_yesterday_date,
    dm_priorday_date,
    dm_ym_last,
    test_alvaro,
    dm_current_week_flag,
    current_month_flag,
    dm_last_week_flag,
    dm_prior_week_flag,
    dm_last_week_last_year_flag,
    sales_yesterday,
    sales_prior_day,
    sales_sdlw,
    sales_ytd,
    sales_mtd,
    sales_last_month_mtd,
    sales_last_year_mtd,
    sales_last_year_ytd,
    sales_current_year,
    sales_last_year,
    sales_prior_month,
    sales_last_week,
    sales_prior_week,
    sales_current_week,
    sales_last_week_last_year,
    sales_last_month,
    units_yesterday,
    units_prior_day,
    units_sdlw,
    units_ytd,
    units_mtd,
    units_last_month_mtd,
    units_last_year_mtd,
    units_least_year_ytd,
    units_month,
    units_prior_month,
    units_last_week,
    units_prior_week,
    units_last_week_last_year,
    units_last_month,
    bb_yesterday,
    bb_prior_day,
    bb_sdlw,
    bb_ytd,
    bb_mtd,
    bb_last_month_mtd,
    bb_last_year_mtd,
    bb_last_year_ytd,
    bb_current_year,
    bb_last_year,
    bb_prior_month,
    bb_last_week,
    bb_prior_week,
    bb_last_week_last_year,
    bb_last_month,
    cvr_yesterday,
    cvr_prior_day,
    cvr_sdlw,
    cvr_ytd,
    cvr_mtd,
    cvr_last_month_mtd,
    cvr_last_year_mtd,
    cvr_least_year_ytd,
    cvr_month,
    cvr_last_month,
    cvr_last_week,
    cvr_prior_week,
    cvr_last_week_last_year,
    cvr_prior_month,
    sessions_yesterday,
    sessions_prior_day,
    sessions_sdlw,
    sessions_ytd,
    sessions_mtd,
    sessions_last_month_mtd,
    sessions_last_year_mtd,
    sessions_least_year_ytd,
    sessions_month,
    sessions_last_month,
    sessions_last_week,
    sessions_prior_week,
    sessions_last_week_last_year,
    sessions_prior_month,
    ad_spend_yesterday,
    ad_spend_prior_day,
    ad_spend_sdlw,
    ad_spend_ytd,
    ad_spend_mtd,
    ad_spend_last_month_mtd,
    ad_spend_last_year_mtd,
    ad_spend_last_year_ytd,
    ad_spend_current_year,
    ad_spend_last_year,
    ad_spend_prior_month,
    ad_spend_last_week,
    ad_spend_prior_week,
    ad_spend_current_week,
    ad_spend_last_week_last_year,
    ad_spend_last_month,
    ad_sales_yesterday,
    ad_sales_prior_day,
    ad_sales_sdlw,
    ad_sales_ytd,
    ad_sales_mtd,
    ad_sales_last_month_mtd,
    ad_sales_last_year_mtd,
    ad_sales_last_year_ytd,
    ad_sales_current_year,
    ad_sales_last_year,
    ad_sales_prior_month,
    ad_sales_last_week,
    ad_sales_prior_week,
    ad_sales_current_week,
    ad_sales_last_week_last_year,
    ad_sales_last_month,
    no_inv_days_yesterday,
    no_inv_days_prior_day,
    no_inv_days_sdlw,
    no_inv_days_ytd,
    no_inv_days_mtd,
    no_inv_days_last_month_mtd,
    no_inv_days_last_year_mtd,
    no_inv_days_last_year_ytd,
    no_inv_days_current_year,
    no_inv_days_last_year,
    no_inv_days_prior_month,
    no_inv_days_last_week,
    no_inv_days_prior_week,
    no_inv_days_current_week,
    no_inv_days_last_week_last_year,
    no_inv_days_last_month,
    ad_clicks_yesterday,
    ad_clicks_prior_day,
    ad_clicks_sdlw,
    ad_clicks_ytd,
    ad_clicks_mtd,
    ad_clicks_last_month_mtd,
    ad_clicks_last_year_mtd,
    ad_clicks_last_year_ytd,
    ad_clicks_current_year,
    ad_clicks_last_year,
    ad_clicks_prior_month,
    ad_clicks_last_week,
    ad_clicks_prior_week,
    ad_clicks_current_week,
    ad_clicks_last_week_last_year,
    ad_clicks_last_month,
    ad_orders_yesterday,
    ad_orders_prior_day,
    ad_orders_sdlw,
    ad_orders_ytd,
    ad_orders_mtd,
    ad_orders_last_month_mtd,
    ad_orders_last_year_mtd,
    ad_orders_last_year_ytd,
    ad_orders_current_year,
    ad_orders_last_year,
    ad_orders_prior_month,
    ad_orders_last_week,
    ad_orders_prior_week,
    ad_orders_current_week,
    ad_orders_last_week_last_year,
    ad_orders_last_month,
    current_bsr,
    current_customer_rating,
    amazon_parent_category,
    amazon_child_category_01,
    amazon_child_category_02,
    amazon_child_category_03,
    amazon_child_category_04,
    report_date,
    last_month_sales_sum,
    last_month_units_sum,
    ytd_sales_sum,
    ytd_units_sum,
    mtd_sales_sum,
    mtd_units_sum,
    last_mtd_sales_sum,
    last_mtd_units_sum,
    last_ytd_sales_sum,
    last_ytd_units_sum,
    prior_month_sales_sum,
    prior_month_units_sum,
    prior_mtd_sales_sum,
    prior_mtd_units_sum,
    last_month_sales_rank,
    last_month_units_rank,
    ytd_sales_rank,
    ytd_units_rank,
    mtd_sales_rank,
    mtd_units_rank,
    last_mtd_sales_rank,
    last_mtd_units_rank,
    last_ytd_sales_rank,
    last_ytd_units_rank,
    prior_month_sales_rank,
    prior_month_units_rank,
    prior_mtd_sales_rank,
    prior_mtd_units_rank,
    ly_cm_sales_rank,
    ly_cm_units_rank,
    brand_ytd_sales_%,
    portfolio_short_name,
    portfolio_long_name,
    product_name,
    partner,
    brand,
    revenue_flag,
    sales_rank_3mth,
    %_suppressed_by_day,
    scraper_date
  FROM Daily_Sales_Traffic_Data_DS
), _Inventory_Velocity_Daily_ASIN_Trend_DS AS (
  SELECT
    asin,
    amazon_seller_id,
    country_id,
    date_of_first_sale,
    purchase_date,
    amazon_inventory_log_country,
    afn_fulfillable_quantity,
    reserved_fc_transfers,
    reserved_fc_processing,
    inbound_receiving,
    inbound_checked_in,
    inbound_delivered,
    inbound_in_transit,
    inbound_working,
    inbound_shipped,
    po_created,
    po_approved,
    po_invoiced,
    po_received,
    po_processed,
    on_order,
    fba_mfn_available,
    local_on_hand,
    asin_cnt,
    brand,
    partner,
    is_discontinued_status,
    map,
    msrp,
    no_inventory,
    no_inv_n_rf_proc,
    noinventorycount_28days,
    noinventorycount_21days,
    noinventorycount_14days,
    noinventorycount_07days,
    noinventorycount_56days,
    noinventorycount_84days,
    sale_date,
    units_asin_a,
    units_upc_a,
    revenue_a,
    vendor_sku,
    asin_name,
    units_asin,
    units_upc,
    revenue,
    units_asin_2,
    l28_revenue,
    l28_demand,
    l28_no_inventory,
    l56_demand,
    l56_no_inventory,
    l84_demand,
    l84_no_inventory,
    count_28_days,
    countr_56_days,
    l7_demand,
    l7_no_inventory,
    full_oos_day,
    partial_oos_day,
    l28_normalized,
    l56_normalized,
    l28_normalized_cnt,
    l56_normalized_cnt,
    dow,
    l07_actual,
    yq,
    relative_date,
    units_asin_stdev,
    units_upc_stdev,
    units_asin_median,
    units_upc_median,
    units_asin_avg,
    units_upc_avg,
    avg_price_90day,
    max_demand,
    p90_units,
    min_nonzero_demand,
    p20_units,
    p80_units,
    calculated_days,
    p10_units,
    p95_units,
    p97_units,
    ltv_units_asin_stdev,
    ltv_units_asin_avg,
    ltv_units_upc_avg,
    ltv_avg_price,
    ltv_p90_units,
    ltv_p20_units,
    ltv_p80_units,
    back_fill_avg,
    back_fill_type,
    noise,
    back_fill,
    ads_90d,
    lt_1stdev,
    total_inventory,
    iad_prep,
    iad,
    iad_m80,
    iad_classification,
    iad_capped,
    full_day_cost_iad,
    partial_day_cost_iad,
    total_cost_iad,
    is_adjusted,
    is_adjusted_capped,
    l28_cnt_days,
    l14_cnt_days,
    l07_cnt_days,
    iad_adjusted_count,
    iad_adjusted_cap_count,
    l28_iad_m80,
    l28_iad,
    l14_iad,
    l07_iad,
    l07_iad_m80,
    l28_iad_cap,
    l28_ilo_value,
    daterank,
    l28_cap_lag07,
    l28_cap_lag01,
    l28_cap_lag28,
    iad_r28,
    iad_r28m,
    iad_r14,
    iad_r07,
    iad_r07m,
    iad_cap_r28,
    iad_cap_r28_lag07,
    iad_cap_r28_lag01,
    iad_cap_r28_lag28,
    dod_av1_trend,
    wow_av1_trend,
    mom_av1_trend,
    iad_28_is_adjusted,
    iad_28_is_adjusted_capped,
    child_asin,
    amazon_seller_central_id,
    aap_country_id,
    page_views_owned,
    page_view_total,
    bbp_max,
    bbp_avg,
    buy_box_percentage,
    av1,
    av2,
    av3,
    dod_change_flag,
    wow_change_flag,
    mom_change_flag,
    purchaseym,
    date,
    rank_and_window_5_amazon_seller_central_id,
    rank_and_window_5_aap_country_id,
    year-qtr,
    year,
    bb_r28_avg,
    bb_pv_owned_sum_28,
    bb_pv_total_sum_28,
    bb-q_1_aap_country_id,
    bb-q_1_amazon_seller_central_id,
    bb_p15q,
    bb_p75q,
    bb_avgq,
    bb_p50q,
    bb_sdq,
    bb-y_aap_country_id,
    bb-y_amazon_seller_central_id,
    bb_p15y,
    bb_p75y,
    bb_avgy,
    bb_p50y,
    bb_sdy,
    bb-all_time_child_asin,
    bb-all_time_aap_country_id,
    bb-all_time_amazon_seller_central_id,
    bb_p15_at,
    bb_p75_at,
    bb_avg_at,
    bb_p50_at,
    bb_sd_at,
    oos_risk_flag,
    accountname,
    product_name,
    parent_category,
    child_category,
    child_category_2,
    child_category_3,
    vel_status_bins,
    select_columns_brand,
    partner_name,
    av1_bins,
    doh_bins,
    catalog_last_appearence,
    business_unit,
    active_reorder_status_count,
    upc_reorder,
    cost,
    inv_age_0_to_90_days,
    inv_age_91_to_180_days,
    inv_age_181_to_270_days,
    inv_age_271_to_365_days1,
    inv_age_365_plus_days,
    sku_cost,
    select_columns_map,
    srp,
    currency,
    country_name,
    domain_sales_channel,
    country_name_short,
    ad_id,
    bbad_p50q,
    bbad_p50q_valuation,
    bbad_r28,
    bbad_r28_valuation,
    bbad_p50_dmd_units,
    bbad_p50_est_units,
    bbad_p50_new_bb,
    bbad_m80_units,
    bbad_m80_valuation,
    loss_opportunity_by_type,
    loss_opportunity_by_type_m80,
    inventory_fault_valuation,
    buy_box_fault_valuation,
    country,
    hero,
    fba_fulfillable_doh,
    inbound_@_amazon_doh,
    inbound_in_transit_doh,
    rc_doh,
    on_order_doh,
    target_doc,
    brand_status,
    purchaser,
    active_purchasing,
    hero_target_doc,
    total_inventory_doh,
    overstock_flag,
    overstock_value,
    baseline_value,
    overstock_qty
  FROM Inventory_Velocity_Daily_ASIN_Trend_DS
), _Enforcement_DS AS (
  SELECT
    upc,
    vendor_code,
    catalog_map,
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
    asin_active,
    asin_offers_scraper_enabled,
    asin_hero_configuration,
    asin_transparency_status,
    country,
    sales_channel,
    date,
    day_of_week,
    month,
    445_week,
    445_week_number,
    445_month,
    445_month_number,
    445_calendar,
    445_year,
    bbc_original_status,
    bbc_2_8_22_status,
    seller_id,
    storefront_enforceable,
    storefront_enforceable_code,
    marketplace_channel_id,
    marketplace_channel,
    id,
    enforcement_configuration_id,
    brand_assigned_user,
    brand_assigned_user_initials,
    seller_brands_cols_storefront_enforceable,
    brand_enforceable,
    assigned_user_id,
    enforce_by,
    group_partner_brand,
    storefront_id,
    partner,
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
    enter_date,
    new_notes,
    new_invoice_id
  FROM Enforcement_DS
), _IDQ_ASIN_Performance_DS AS (
  SELECT
    select_columns_1_brand,
    asin,
    idq_score,
    date,
    title,
    review_ratings,
    review_avg_rating,
    idq_grade,
    amazon_asin_id,
    client_id,
    brand_id,
    client_name,
    brand
  FROM IDQ_ASIN_Performance_DS
), _RADAR_COBALT_INPUT AS (
  SELECT
    brand,
    start_date,
    brand_sales,
    anchor,
    greatest_date,
    brand_core_category,
    category_sales,
    dm_current_date,
    dm_relative_date,
    dm_year,
    dm_ym,
    dm_month,
    dm_month_full_name,
    dm_month_short_name,
    dm_today_flag,
    dm_pr_yr_wk,
    dm_current_month_flag,
    dm_last_month_flag,
    dm_prior_month_flag,
    dm_last_year_month_flag,
    dm_prior_year_month_flag,
    dm_ytd_flag,
    dm_last_ytd_flag,
    dm_mtd_flag,
    dm_last_mtd_flag,
    dm_last_year_mtd_flag,
    dm_domo_current_date,
    dm_domo_current_time,
    dm_domo_current_timestamp,
    dm_month_int,
    dm_current_year_flag,
    dm_last_year_flag,
    dm_last_12_flag,
    dm_prior_12_flag,
    dm_last_3_month_flag,
    dm_last_p3_month_flag,
    dm_p3_max_date,
    brand_sales_ytd,
    category_sales_ytd,
    brand_sales_last_12,
    category_sales_last_12,
    brand_sales_prior_12,
    category_sales_prior_12,
    brand_sales_current_month,
    category_sales_current_month,
    brand_sales_last_month,
    category_sales_last_month,
    brand_sales_last_ytd,
    category_sales_last_ytd,
    brand_sales_last_3mth,
    category_sales_last_3mth,
    brand_sales_prior_3mth,
    category_sales_prior_3mth
  FROM aDESTROY_RADAR_COBALT_INPUT
), _Cleaned_up_Scrapped_Cat_Leaf_Ratings AS (
  SELECT
    asin,
    bb_information,
    best_seller_rank,
    categories,
    created_at,
    fastest_delivery,
    features,
    id,
    images,
    important_information,
    is_404,
    is_out_of_stock,
    offers,
    price,
    product_details,
    product_overview,
    rating,
    detail_rating,
    total_review_count,
    customers_say,
    review_tags,
    redirect_asin,
    return_policy,
    seller_id,
    storefront,
    title,
    updated_at,
    videos,
    country_id,
    full_text_generated,
    seller_name,
    velocity,
    sales,
    fba_inventory,
    batch_id,
    batch_last_run,
    cleaned-up_category_path,
    cleaned_up_ratings_step_1,
    parent_node,
    leaf_node_01,
    leaf_node_02,
    leaf_node_03,
    leaf_node_04,
    brand,
    partner,
    sku,
    sku_count,
    sales_ytd,
    units_ytd
  FROM Cleaned_up_Scrapped_Cat_Leaf_Ratings
), _Advertising_Metrics_All_Orders AS (
  SELECT
    id,
    customerid,
    customername,
    brandname,
    category,
    adtype,
    competitortactic,
    campaignid,
    campaignname,
    targetingtype,
    adgroupid,
    adgroupname,
    keywordid,
    keywordtext,
    matchtype,
    impressions,
    clicks,
    cost,
    cpc,
    ctr,
    revenue,
    roas,
    acos,
    costperconv,
    convrate,
    orders,
    units,
    ntborders,
    ntbordersperc,
    ntbunits,
    ntbunitsperc,
    ntbsales,
    ntbsalesperc,
    opc,
    branddayocurrences,
    date,
    brand_id,
    branddayorderssold,
    branddaysalesamount,
    dpv,
    portfolioid,
    portfolioname,
    dsp_total_product_sales,
    dsp_total_purchases,
    dsp_total_units_sold,
    dpvr,
    currencycode,
    portfolio_client_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    targettingtype,
    category_v2,
    targetting_level,
    targetingmethod,
    category_-_new,
    ad_type_-_new,
    targeting_type_-_new,
    short_name,
    campaign_label_-_tactic,
    strategy_-_new,
    keyword_name,
    advertising_tactic,
    table,
    month,
    year,
    ocurrenciesbymonth,
    dateym,
    calculated_ntborders,
    calculated_ntbsales,
    brand,
    countorders,
    countbuyeremail,
    finalopc,
    group_by_1_brand,
    purchase_date,
    avg_opc,
    targeting_type
  FROM Advertising_Metrics_All_Orders
), _Filter_Rows AS (
  SELECT
    *
  FROM _Daily_Sales_Traffic_Data_DS
  WHERE
    dm_relative_date >= '365'
), _Last_28_D_1 AS (
  SELECT
    *
  FROM _Daily_Sales_Traffic_Data_DS
  WHERE
    dm_relative_date >= '28'
), _Group_By_11 AS (
  SELECT
    brand_name,
    SUM(sales) AS brand_sales,
    SUM(brand_sales_last_12m) AS brand_sales_last_12m,
    SUM(brand_sales_prior_12m) AS brand_sales_prior_12m,
    SUM(sales_ytd) AS brand_sales_cy_ytd,
    SUM(sales_last_year_ytd) AS brand_sales_py_ytd
  FROM _Daily_Sales_Traffic_Data_DS
  GROUP BY
    brand_name
), _Last_28_D AS (
  SELECT
    *
  FROM _Inventory_Velocity_Daily_ASIN_Trend_DS
  WHERE
    relative_date >= '-28'
), _Filter_Rows_3 AS (
  SELECT
    *
  FROM _Enforcement_DS
  WHERE
    TO_DATE(scraped_date) = TO_DATE(DATEADD(DAY, '3' * -1, DATEADD(HOUR, '8' * -1, CURRENT_TIMESTAMP())))
), _Group_By_4 AS (
  SELECT
    asin,
    MAX(date) AS last_run_date
  FROM _IDQ_ASIN_Performance_DS
  GROUP BY
    asin
), _Group_By_8 AS (
  SELECT
    brand,
    SUM(brand_sales_ytd) AS cobalt_brand_sales_ytd,
    SUM(category_sales_ytd) AS category_sales_ytd,
    SUM(brand_sales_last_12) AS cobalt_brand_sales_last_12,
    SUM(category_sales_last_12) AS category_sales_last_12,
    SUM(brand_sales_prior_12) AS cobalt_brand_sales_prior_12,
    SUM(category_sales_prior_12) AS category_sales_prior_12,
    SUM(cobalt_brand_l12_p12) AS cobalt_brand_l12_p12,
    SUM(category_l12_p12) AS category_l12_p12,
    SUM(cobalt_brand_mom) AS cobalt_brand_mom,
    SUM(category_mom) AS category_mom,
    SUM(cobalt_index12m) AS cobalt_index12m,
    SUM(cobalt_indexm) AS cobalt_indexm,
    SUM(category_lytd_pytd) AS category_lytd_pytd,
    SUM(cobalt_brand_lytd_pytd) AS cobalt_brand_lytd_pytd
  FROM _RADAR_COBALT_INPUT
  GROUP BY
    brand
), _Filter_Rows_2 AS (
  SELECT
    *
  FROM _Advertising_Metrics_All_Orders
  WHERE
    date >= DATEADD(DAY, '28' * -1, CURRENT_DATE)
), _Group_By AS (
  SELECT
    country_id,
    asin,
    brand_name,
    SUM(sales) AS sales_last_365_days_asin
  FROM _Filter_Rows
  GROUP BY
    country_id,
    asin,
    brand_name
), _Group_By_2 AS (
  SELECT
    country_id,
    brand_name,
    SUM(sales) AS sales_last_365_days_brand
  FROM _Filter_Rows
  GROUP BY
    country_id,
    brand_name
), _Buy_Box_Branch AS (
  SELECT
    country_id,
    brand_name,
    asin,
    COUNT(DISTINCT purchase_date) AS number_of_unique_days,
    AVG(buy_box_percentage) AS buy_box,
    SUM(sales) AS sales_asin_bb
  FROM _Last_28_D_1
  GROUP BY
    country_id,
    brand_name,
    asin
), _Buy_Box_Branch_1 AS (
  SELECT
    country_id,
    brand_name,
    SUM(sales) AS sales_brand_bb
  FROM _Last_28_D_1
  GROUP BY
    country_id,
    brand_name
), _Group_By_1 AS (
  SELECT
    country_id,
    brand,
    asin,
    SUM(no_inventory) AS no_inventory_sum,
    AVG(no_inventory) AS no_inventory_avg,
    SUM(in-stock_rate_avg) AS in-stock_rate_avg,
    COUNT(DISTINCT purchase_date) AS number_of_unique_days
  FROM _Last_28_D
  GROUP BY
    country_id,
    brand,
    asin
), _Group_By_9 AS (
  SELECT
    country_id,
    brand,
    asin,
    COUNT(DISTINCT seller_id) AS number_of_sellers
  FROM _Filter_Rows_3
  GROUP BY
    country_id,
    brand,
    asin
), _Join_Data_2 AS (
  SELECT
    _IDQ_ASIN_Performance_DS.select_columns_1_brand,
    _IDQ_ASIN_Performance_DS.asin,
    _IDQ_ASIN_Performance_DS.idq_score,
    _IDQ_ASIN_Performance_DS.date,
    _IDQ_ASIN_Performance_DS.title,
    _IDQ_ASIN_Performance_DS.review_ratings,
    _IDQ_ASIN_Performance_DS.review_avg_rating,
    _IDQ_ASIN_Performance_DS.idq_grade,
    _IDQ_ASIN_Performance_DS.amazon_asin_id,
    _IDQ_ASIN_Performance_DS.client_id,
    _IDQ_ASIN_Performance_DS.brand_id,
    _IDQ_ASIN_Performance_DS.client_name,
    _IDQ_ASIN_Performance_DS.brand,
    _Group_By_4.last_run_date
  FROM _IDQ_ASIN_Performance_DS
  LEFT JOIN _Group_By_4
    ON _IDQ_ASIN_Performance_DS.asin = _Group_By_4.asin
), _Join_Data_6 AS (
  SELECT
    _Group_By_11.brand_name,
    _Group_By_11.brand_sales,
    _Group_By_11.brand_sales_last_12m,
    _Group_By_11.brand_sales_prior_12m,
    _Group_By_11.brand_sales_cy_ytd,
    _Group_By_11.brand_sales_py_ytd,
    _Group_By_8.brand AS cobalt_brand,
    _Group_By_8.cobalt_brand_sales_ytd,
    _Group_By_8.category_sales_ytd,
    _Group_By_8.cobalt_brand_sales_last_12,
    _Group_By_8.category_sales_last_12,
    _Group_By_8.cobalt_brand_sales_prior_12,
    _Group_By_8.category_sales_prior_12,
    _Group_By_8.cobalt_brand_l12_p12,
    _Group_By_8.category_l12_p12,
    _Group_By_8.cobalt_brand_mom,
    _Group_By_8.category_mom,
    _Group_By_8.cobalt_index12m,
    _Group_By_8.cobalt_indexm,
    _Group_By_8.category_lytd_pytd,
    _Group_By_8.cobalt_brand_lytd_pytd
  FROM _Group_By_11
  LEFT JOIN _Group_By_8
    ON _Group_By_11.brand_name = _Group_By_8.brand
), _Group_By_5 AS (
  SELECT
    brandname,
    SUM(cost) AS ad_spend,
    SUM(revenue) AS ad_revenue,
    SUM(acos) AS acos,
    SUM(ad_asp) AS ad_asp,
    SUM(roas) AS roas,
    SUM(roas_per_asp) AS roas_per_asp
  FROM _Filter_Rows_2
  GROUP BY
    brandname
), _Sales_365 AS (
  SELECT
    _Group_By.country_id,
    _Group_By.asin,
    _Group_By.brand_name,
    _Group_By.sales_last_365_days_asin,
    _Group_By_2.sales_last_365_days_brand
  FROM _Group_By
  LEFT JOIN _Group_By_2
    ON _Group_By.brand_name = _Group_By_2.brand_name
    AND _Group_By.country_id = _Group_By_2.country_id
), _Join_Data_3 AS (
  SELECT
    _Buy_Box_Branch.country_id,
    _Buy_Box_Branch.brand_name,
    _Buy_Box_Branch.asin,
    _Buy_Box_Branch.number_of_unique_days,
    _Buy_Box_Branch.buy_box,
    _Buy_Box_Branch.sales_asin_bb,
    _Buy_Box_Branch_1.sales_brand_bb
  FROM _Buy_Box_Branch
  LEFT JOIN _Buy_Box_Branch_1
    ON _Buy_Box_Branch.brand_name = _Buy_Box_Branch_1.brand_name
    AND _Buy_Box_Branch.country_id = _Buy_Box_Branch_1.country_id
), _latest_data AS (
  SELECT
    *
  FROM _Join_Data_2
  WHERE
    date >= last_run_date
), _Add_Formula_5 AS (
  SELECT
    brand_name,
    brand_sales,
    brand_sales_last_12m,
    brand_sales_prior_12m,
    brand_sales_cy_ytd,
    brand_sales_py_ytd,
    cobalt_brand,
    cobalt_brand_sales_ytd,
    category_sales_ytd,
    cobalt_brand_sales_last_12,
    category_sales_last_12,
    cobalt_brand_sales_prior_12,
    category_sales_prior_12,
    cobalt_brand_l12_p12,
    category_l12_p12,
    cobalt_brand_mom,
    category_mom,
    cobalt_index12m,
    cobalt_indexm,
    category_lytd_pytd,
    cobalt_brand_lytd_pytd,
    (
      brand_sales_last_12m / NULLIF(brand_sales_prior_12m, 0)
    ) AS brand_l12_p12,
    (
      brand_sales_cy_ytd / NULLIF(brand_sales_py_ytd, 0)
    ) AS brand_lytd_pytd,
    (
      CASE WHEN cobalt_brand IS NULL THEN 1 ELSE 0 END
    ) AS no_cobalt_match_flag,
    (
      cobalt_brand_l12_p12 / NULLIF(cobalt_brand_sales_prior_12, 0)
    ) AS cobalt_brand_l12_p12,
    (
      brand_l12_p12 / NULLIF(category_l12_p12, 0)
    ) AS brand_category_index_12m,
    (
      brand_lytd_pytd / NULLIF(category_lytd_pytd, 0)
    ) AS brand_category_index_ytd,
    (
      cobalt_brand_l12_p12 / NULLIF(category_l12_p12, 0)
    ) AS cobalt_brand_cat_index_12
  FROM _Join_Data_6
), _Add_Formula_3 AS (
  SELECT
    brandname,
    ad_spend,
    ad_revenue,
    acos,
    ad_asp,
    roas,
    roas_per_asp,
    (
      ROUND((
        1 - acos
      ) * 100, 0)
    ) AS metric_value,
    (
      'ACOS'
    ) AS metric,
    (
      'Ads'
    ) AS element,
    (
      1
    ) AS country_id
  FROM _Group_By_5
), _Join_Data AS (
  SELECT
    _Group_By_1.country_id,
    _Group_By_1.brand,
    _Group_By_1.asin,
    _Group_By_1.no_inventory_sum,
    _Group_By_1.no_inventory_avg,
    _Group_By_1.in-stock_rate_avg,
    _Group_By_1.number_of_unique_days,
    _Sales_365.brand_name,
    _Sales_365.sales_last_365_days_asin,
    _Sales_365.sales_last_365_days_brand
  FROM _Group_By_1
  LEFT JOIN _Sales_365
    ON _Group_By_1.brand = _Sales_365.brand_name
    AND _Group_By_1.country_id = _Sales_365.country_id
    AND _Group_By_1.asin = _Sales_365.asin
), _Join_Data_11 AS (
  SELECT
    _Join_Data_3.country_id,
    _Join_Data_3.brand_name,
    _Join_Data_3.asin,
    _Join_Data_3.number_of_unique_days,
    _Join_Data_3.buy_box,
    _Join_Data_3.sales_asin_bb,
    _Join_Data_3.sales_brand_bb,
    _Group_By_9.number_of_sellers
  FROM _Join_Data_3
  LEFT JOIN _Group_By_9
    ON _Join_Data_3.asin = _Group_By_9.asin
    AND _Join_Data_3.brand_name = _Group_By_9.brand
    AND _Join_Data_3.country_id = _Group_By_9.country_id
), _Filter_Rows_1 AS (
  SELECT
    *
  FROM _Join_Data_3
  WHERE
    country_id = '1'
), _Add_Formula_2 AS (
  SELECT
    select_columns_1_brand,
    asin,
    idq_score,
    date,
    title,
    review_ratings,
    review_avg_rating,
    idq_grade,
    amazon_asin_id,
    client_id,
    brand_id,
    client_name,
    brand,
    last_run_date,
    (
      CASE WHEN idq_score < 1 THEN idq_score * 100 ELSE idq_score END
    ) AS idq_normalized
  FROM _latest_data
), _Add_Formula_6 AS (
  SELECT
    brand_name,
    brand_sales,
    brand_sales_last_12m,
    brand_sales_prior_12m,
    brand_sales_cy_ytd,
    brand_sales_py_ytd,
    cobalt_brand,
    cobalt_brand_sales_ytd,
    category_sales_ytd,
    cobalt_brand_sales_last_12,
    category_sales_last_12,
    cobalt_brand_sales_prior_12,
    category_sales_prior_12,
    cobalt_brand_l12_p12,
    category_l12_p12,
    cobalt_brand_mom,
    category_mom,
    cobalt_index12m,
    cobalt_indexm,
    category_lytd_pytd,
    cobalt_brand_lytd_pytd,
    brand_l12_p12,
    brand_lytd_pytd,
    brand_category_index_12m,
    brand_category_index_ytd,
    no_cobalt_match_flag,
    cobalt_brand_l12_p12,
    cobalt_brand_cat_index_12,
    (
      'Momentum'
    ) AS element,
    (
      CASE
        WHEN brand_category_index_ytd > 1 AND brand_category_index_12m > 1
        THEN 100
        WHEN brand_category_index_ytd > 0.75 AND brand_category_index_12m > 0.75
        THEN 75
        WHEN brand_category_index_ytd > 1 AND brand_category_index_12m < 1
        THEN 65
        WHEN brand_category_index_ytd < 1 AND brand_category_index_12m > 1
        THEN 50
        WHEN brand_category_index_ytd > 0.5 AND brand_category_index_12m > 0.5
        THEN 50
        WHEN brand_category_index_ytd < 0.5 AND brand_category_index_12m < 0.5
        THEN 25
        WHEN brand_category_index_ytd < 0.5
        THEN 25
        ELSE 55
      END
    ) AS metric,
    (
      1
    ) AS country_id
  FROM _Add_Formula_5
), _Select_Columns_11 AS (
  SELECT
    brandname AS brand,
    metric_value,
    element,
    country_id
  FROM _Add_Formula_3
), _Select_Columns_15 AS (
  SELECT
    brandname AS brand,
    metric_value AS ads,
    country_id
  FROM _Add_Formula_3
), _Add_Formula AS (
  SELECT
    country_id,
    brand,
    asin,
    no_inventory_sum,
    no_inventory_avg,
    in-stock_rate_avg,
    number_of_unique_days,
    brand_name,
    sales_last_365_days_asin,
    sales_last_365_days_brand,
    (
      COALESCE(sales_last_365_days_asin * in-stock_rate_avg, 0)
    ) AS sales_weight,
    (
      'Product Availability'
    ) AS element,
    (
      CASE
        WHEN (
          sales_weight / NULLIF(sales_last_365_days_brand, 0)
        ) IS NULL
        THEN 0
        ELSE (
          sales_weight / NULLIF(sales_last_365_days_brand, 0)
        )
      END
    ) AS in-stock_weight
  FROM _Join_Data
), _Add_Formula_4 AS (
  SELECT
    country_id,
    brand_name,
    asin,
    number_of_unique_days,
    buy_box,
    sales_asin_bb,
    sales_brand_bb,
    number_of_sellers,
    (
      (
        CASE
          WHEN number_of_sellers = 1
          THEN 100
          WHEN number_of_sellers <= 5
          THEN 75
          WHEN number_of_sellers <= 10
          THEN 50
          WHEN number_of_sellers <= 20
          THEN 25
          WHEN number_of_sellers >= 20
          THEN 0
        END
      )
    ) AS seller_points,
    (
      buy_box
    ) AS buy_box_points,
    (
      (
        0.3 * seller_points
      ) + (
        0.7 * buy_box_points
      )
    ) AS market_control_points
  FROM _Join_Data_11
), _Join_Data_10 AS (
  SELECT
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.asin,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.bb_information,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.best_seller_rank,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.categories,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.created_at,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.fastest_delivery,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.features,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.id,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.images,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.important_information,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.is_404,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.is_out_of_stock,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.offers,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.price,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.product_details,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.product_overview,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.rating,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.detail_rating,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.total_review_count,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.customers_say,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.review_tags,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.redirect_asin,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.return_policy,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.seller_id,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.storefront,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.title,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.updated_at,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.videos,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.country_id,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.full_text_generated,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.seller_name,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.velocity,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.sales,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.fba_inventory,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.batch_id,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.batch_last_run,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.cleaned-up_category_path,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.cleaned_up_ratings_step_1,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.parent_node,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.leaf_node_01,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.leaf_node_02,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.leaf_node_03,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.leaf_node_04,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.brand,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.partner,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.sku,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.sku_count,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.sales_ytd,
    _Cleaned_up_Scrapped_Cat_Leaf_Ratings.units_ytd,
    _Filter_Rows_1.brand_name,
    _Filter_Rows_1.number_of_unique_days,
    _Filter_Rows_1.buy_box,
    _Filter_Rows_1.sales_asin_bb,
    _Filter_Rows_1.sales_brand_bb
  FROM _Cleaned_up_Scrapped_Cat_Leaf_Ratings
  LEFT JOIN _Filter_Rows_1
    ON _Cleaned_up_Scrapped_Cat_Leaf_Ratings.brand = _Filter_Rows_1.brand_name
    AND _Cleaned_up_Scrapped_Cat_Leaf_Ratings.asin = _Filter_Rows_1.asin
), _Group_By_7 AS (
  SELECT
    brand,
    asin,
    AVG(idq_normalized) AS idq_score,
    COUNT(DISTINCT date) AS idq_unique_days,
    AVG(review_avg_rating) AS avg_ratings,
    SUM(review_ratings) AS reviewers,
    SUM(avg_ratings_100) AS avg_ratings_100,
    SUM(country_id) AS country_id
  FROM _Add_Formula_2
  GROUP BY
    brand,
    asin
), _Select_Columns_8 AS (
  SELECT
    brand_name AS brand,
    element,
    metric AS metric_value
  FROM _Add_Formula_6
), _Select_Columns_14 AS (
  SELECT
    brand_name AS brand,
    metric AS momentum,
    country_id
  FROM _Add_Formula_6
), _Group_By_3 AS (
  SELECT
    country_id,
    brand,
    element,
    SUM(weighted_brand_in-stock_rate) AS weighted_brand_in-stock_rate
  FROM _Add_Formula
  GROUP BY
    country_id,
    brand,
    element
), _Select_Columns AS (
  SELECT
    country_id,
    asin,
    sales_last_365_days_asin AS sales_asin_weight,
    brand,
    in-stock_rate_avg AS metric,
    sales_weight AS sales_weight,
    in-stock_weight AS metric_weight,
    element,
    sales_last_365_days_brand AS sales_brand_weight
  FROM _Add_Formula
), _Add_Formula_1 AS (
  SELECT
    country_id,
    brand_name,
    asin,
    number_of_unique_days,
    buy_box,
    sales_asin_bb,
    sales_brand_bb,
    number_of_sellers,
    seller_points,
    buy_box_points,
    market_control_points,
    (
      COALESCE(sales_asin_bb * market_control_points, 0)
    ) AS sales_weight,
    (
      'Market Control'
    ) AS element,
    (
      CASE
        WHEN (
          sales_weight / NULLIF(sales_brand_bb, 0)
        ) IS NULL
        THEN 0
        ELSE (
          sales_weight / NULLIF(sales_brand_bb, 0)
        )
      END
    ) AS buy_box_weight
  FROM _Add_Formula_4
), _Ratings AS (
  SELECT
    country_id,
    brand_name,
    asin,
    number_of_unique_days,
    buy_box,
    sales_asin_bb,
    sales_brand_bb,
    bb_information,
    best_seller_rank,
    categories,
    created_at,
    fastest_delivery,
    features,
    id,
    images,
    important_information,
    is_404,
    is_out_of_stock,
    offers,
    price,
    product_details,
    product_overview,
    rating,
    detail_rating,
    total_review_count,
    customers_say,
    review_tags,
    redirect_asin,
    return_policy,
    seller_id,
    storefront,
    title,
    updated_at,
    videos,
    full_text_generated,
    seller_name,
    velocity,
    sales,
    fba_inventory,
    batch_id,
    batch_last_run,
    cleaned-up_category_path,
    cleaned_up_ratings_step_1,
    parent_node,
    leaf_node_01,
    leaf_node_02,
    leaf_node_03,
    leaf_node_04,
    brand,
    partner,
    sku,
    sku_count,
    sales_ytd,
    units_ytd,
    (
      COALESCE(sales_asin_bb * cleaned_up_ratings_step_1 * 20, 0)
    ) AS sales_weight,
    (
      'Customer Satisfaction'
    ) AS element,
    (
      CASE
        WHEN (
          sales_weight / NULLIF(sales_brand_bb, 0)
        ) IS NULL
        THEN 0
        ELSE (
          sales_weight / NULLIF(sales_brand_bb, 0)
        )
      END
    ) AS ratings_weight
  FROM _Join_Data_10
), _Join_Data_1 AS (
  SELECT
    _Group_By_7.brand,
    _Group_By_7.asin,
    _Group_By_7.idq_score,
    _Group_By_7.idq_unique_days,
    _Group_By_7.avg_ratings,
    _Group_By_7.reviewers,
    _Group_By_7.avg_ratings_100,
    _Group_By_7.country_id,
    _Filter_Rows_1.brand_name,
    _Filter_Rows_1.number_of_unique_days,
    _Filter_Rows_1.buy_box,
    _Filter_Rows_1.sales_asin_bb AS sales_asin_idq,
    _Filter_Rows_1.sales_brand_bb AS sales_brand_idq
  FROM _Group_By_7
  LEFT JOIN _Filter_Rows_1
    ON _Group_By_7.brand = _Filter_Rows_1.brand_name
    AND _Group_By_7.asin = _Filter_Rows_1.asin
), _IS_Append AS (
  SELECT
    country_id,
    brand,
    weighted_brand_in-stock_rate AS metric_value,
    element
  FROM _Group_By_3
), _IS_Join AS (
  SELECT
    country_id,
    brand,
    weighted_brand_in-stock_rate AS in-stock_rate
  FROM _Group_By_3
), _Group_By_6 AS (
  SELECT
    country_id,
    brand_name,
    element,
    SUM(weighted_brand_buy_box) AS weighted_brand_buy_box
  FROM _Add_Formula_1
  GROUP BY
    country_id,
    brand_name,
    element
), _Select_Columns_2 AS (
  SELECT
    country_id,
    asin,
    brand_name AS brand,
    number_of_unique_days,
    sales_weight AS sales_weight,
    buy_box AS metric_value,
    buy_box_weight AS metric_weight,
    element,
    sales_asin_bb AS sales_asin_weight,
    sales_brand_bb AS sales_brand_weight
  FROM _Add_Formula_1
), _Group_By_12 AS (
  SELECT
    brand,
    element,
    country_id,
    SUM(weighted_brand_avg_rating) AS weighted_brand_avg_rating
  FROM _Ratings
  GROUP BY
    brand,
    element,
    country_id
), _Select_Columns_5 AS (
  SELECT
    asin,
    brand_name AS brand,
    sales_weight,
    ratings_weight AS metric_weight,
    sales_asin_bb AS sales_asin_weight,
    sales_brand_bb AS sales_brand_weight,
    cleaned_up_ratings_step_1 AS metric
  FROM _Ratings
), _IDQ AS (
  SELECT
    brand,
    asin,
    idq_score,
    idq_unique_days,
    avg_ratings,
    reviewers,
    avg_ratings_100,
    country_id,
    brand_name,
    number_of_unique_days,
    buy_box,
    sales_asin_idq,
    sales_brand_idq,
    (
      COALESCE(sales_asin_idq * idq_score, 0)
    ) AS sales_weight,
    (
      'Creative'
    ) AS element,
    (
      CASE
        WHEN (
          sales_weight / NULLIF(sales_brand_idq, 0)
        ) IS NULL
        THEN 0
        ELSE (
          sales_weight / NULLIF(sales_brand_idq, 0)
        )
      END
    ) AS idq_weight
  FROM _Join_Data_1
), _BB_Append AS (
  SELECT
    country_id,
    brand_name AS brand,
    element,
    weighted_brand_buy_box AS metric_value
  FROM _Group_By_6
), _BB_Join AS (
  SELECT
    country_id,
    brand_name AS brand,
    weighted_brand_buy_box AS buy_box
  FROM _Group_By_6
), _Select_Columns_7 AS (
  SELECT
    brand,
    element,
    weighted_brand_avg_rating AS metric_value,
    country_id
  FROM _Group_By_12
), _Select_Columns_13 AS (
  SELECT
    brand,
    weighted_brand_avg_rating AS avg_rating,
    country_id
  FROM _Group_By_12
), _Group_By_10 AS (
  SELECT
    brand,
    element,
    country_id,
    SUM(weighted_brand_idq) AS weighted_brand_idq
  FROM _IDQ
  GROUP BY
    brand,
    element,
    country_id
), _Select_Columns_4 AS (
  SELECT
    asin,
    idq_score AS metric,
    brand_name AS brand,
    sales_weight,
    idq_weight AS metric_weight,
    sales_asin_idq AS sales_asin_weight,
    sales_brand_idq AS sales_brand_weight
  FROM _IDQ
), _Join_Data_4 AS (
  SELECT
    _IS_Join.country_id,
    _IS_Join.brand,
    _IS_Join.in-stock_rate,
    _BB_Join.buy_box
  FROM _IS_Join
  FULL JOIN _BB_Join
    ON _IS_Join.brand = _BB_Join.brand
    AND _IS_Join.country_id = _BB_Join.country_id
), _IDQ_Append AS (
  SELECT
    brand,
    element,
    weighted_brand_idq AS metric_value,
    country_id
  FROM _Group_By_10
), _IDQ_Join AS (
  SELECT
    brand,
    weighted_brand_idq AS idq,
    country_id
  FROM _Group_By_10
), _Append_Rows AS (
  SELECT
    country_id,
    asin,
    sales_asin_weight,
    brand,
    metric,
    sales_weight,
    metric_weight,
    element,
    sales_brand_weight
  FROM _Select_Columns
  UNION ALL
  SELECT
    country_id,
    asin,
    sales_asin_weight,
    brand,
    CAST(NULL AS DECIMAL(38, 0)) AS metric,
    sales_weight,
    metric_weight,
    element,
    sales_brand_weight
  FROM _Select_Columns_2
  UNION ALL
  SELECT
    CAST(NULL AS BIGINT) AS country_id,
    asin,
    sales_asin_weight,
    brand,
    metric,
    sales_weight,
    metric_weight,
    CAST(NULL AS VARCHAR) AS element,
    sales_brand_weight
  FROM _Select_Columns_4
  UNION ALL
  SELECT
    CAST(NULL AS BIGINT) AS country_id,
    asin,
    sales_asin_weight,
    brand,
    CAST(metric AS DECIMAL(38, 0)) AS metric,
    sales_weight,
    metric_weight,
    CAST(NULL AS VARCHAR) AS element,
    sales_brand_weight
  FROM _Select_Columns_5
), _Append_Rows_1 AS (
  SELECT
    country_id,
    brand,
    metric_value,
    element
  FROM _IS_Append
  UNION ALL
  SELECT
    country_id,
    brand,
    metric_value,
    element
  FROM _BB_Append
  UNION ALL
  SELECT
    CAST(country_id AS BIGINT) AS country_id,
    brand,
    metric_value,
    element
  FROM _IDQ_Append
  UNION ALL
  SELECT
    country_id,
    brand,
    metric_value,
    element
  FROM _Select_Columns_7
  UNION ALL
  SELECT
    CAST(NULL AS BIGINT) AS country_id,
    brand,
    CAST(metric_value AS DECIMAL(38, 0)) AS metric_value,
    element
  FROM _Select_Columns_8
  UNION ALL
  SELECT
    CAST(country_id AS BIGINT) AS country_id,
    brand,
    metric_value,
    element
  FROM _Select_Columns_11
), _Join_Data_5 AS (
  SELECT
    _Join_Data_4.country_id,
    _Join_Data_4.brand,
    _Join_Data_4.in-stock_rate,
    _Join_Data_4.buy_box,
    _IDQ_Join.idq
  FROM _Join_Data_4
  FULL JOIN _IDQ_Join
    ON _Join_Data_4.brand = _IDQ_Join.brand
), _Select_Columns_10 AS (
  SELECT
    country_id,
    asin,
    sales_asin_weight,
    brand AS brand_name,
    metric,
    sales_weight,
    metric_weight,
    element,
    sales_brand_weight,
    number_of_unique_days,
    metric_value
  FROM _Append_Rows
), _Select_Columns_9 AS (
  SELECT
    country_id,
    brand AS brand_name,
    metric_value,
    element
  FROM _Append_Rows_1
), _Join_Data_7 AS (
  SELECT
    _Join_Data_5.country_id,
    _Join_Data_5.brand,
    _Join_Data_5.in-stock_rate,
    _Join_Data_5.buy_box,
    _Join_Data_5.idq,
    _Select_Columns_13.avg_rating
  FROM _Join_Data_5
  FULL JOIN _Select_Columns_13
    ON _Join_Data_5.brand = _Select_Columns_13.brand
), _Radar_ASIN_Level_Metrics AS (
  SELECT
    *
  FROM _Select_Columns_10
), _Radar_BRAND_Level_Append_Metrics AS (
  SELECT
    *
  FROM _Select_Columns_9
), _Join_Data_8 AS (
  SELECT
    _Join_Data_7.country_id,
    _Join_Data_7.brand,
    _Join_Data_7.in-stock_rate,
    _Join_Data_7.buy_box,
    _Join_Data_7.idq,
    _Join_Data_7.avg_rating,
    _Select_Columns_14.momentum,
    _Select_Columns_14.country_id AS select_columns_14_country_id
  FROM _Join_Data_7
  LEFT JOIN _Select_Columns_14
    ON _Join_Data_7.brand = _Select_Columns_14.brand
), _Join_Data_9 AS (
  SELECT
    _Join_Data_8.country_id,
    _Join_Data_8.brand,
    _Join_Data_8.in-stock_rate,
    _Join_Data_8.buy_box,
    _Join_Data_8.idq,
    _Join_Data_8.avg_rating,
    _Join_Data_8.momentum,
    _Join_Data_8.select_columns_14_country_id,
    _Select_Columns_15.brand AS select_columns_15_brand,
    _Select_Columns_15.ads,
    _Select_Columns_15.country_id AS select_columns_15_country_id
  FROM _Join_Data_8
  FULL JOIN _Select_Columns_15
    ON _Join_Data_8.brand = _Select_Columns_15.brand
), _Radar_Brand_Level_Join_Metrics AS (
  SELECT
    *
  FROM _Join_Data_9
)
SELECT
  *
FROM _Radar_Brand_Level_Join_Metrics