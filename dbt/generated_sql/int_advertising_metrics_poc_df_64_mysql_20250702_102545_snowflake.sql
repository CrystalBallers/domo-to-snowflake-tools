/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 64
Dataflow Name: Advertising Metrics POC DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 36
  Successful: 36
  Failed: 0
  Unique Action Types: 8
  Action Types: ExpressionEvaluator, Filter, GroupBy, LoadFromVault, MergeJoin, PublishToVault, SelectValues, UnionAll
  Pipelines: 11

Generated: 2025-07-02 10:25:49
================================================================================
*/
WITH _advertising_metrics AS (
  SELECT
    ad_group_id,
    ad_group_name,
    attributed_conversions_14d,
    attributed_conversions_14d_same_sku,
    attributed_conversions_7d,
    attributed_conversions_7d_same_sku,
    attributed_detail_page_views_clicks_14d,
    attributed_order_rate_new_to_brand_14d,
    attributed_orders_new_to_brand_14d,
    attributed_orders_new_to_brand_percentage_14d,
    attributed_sales_14d,
    attributed_sales_14d_same_sku,
    attributed_sales_7d,
    attributed_sales_7d_same_sku,
    attributed_sales_new_to_brand_14d,
    attributed_sales_new_to_brand_percentage_14d,
    attributed_units_ordered_7d,
    attributed_units_ordered_7d_same_sku,
    attributed_units_ordered_new_to_brand_14d,
    attributed_units_ordered_new_to_brand_percentage_14d,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    created_at,
    date,
    dpv_14d,
    external_id,
    external_name,
    id,
    impressions,
    portfolio_id,
    table,
    units_sold_14d,
    updated_at,
    dsp_total_units_sold,
    dsp_total_purchases,
    dsp_total_product_sales,
    ad_type,
    metric_deleted_at,
    batch_id,
    batch_last_run
  FROM advertising_metrics
), _advertising_campaigns AS (
  SELECT
    bidding_strategy,
    campaign_id,
    category,
    created_at,
    deleted_at,
    end_date,
    id,
    name,
    portfolio_id,
    promotion,
    start_date,
    state,
    targeting_type,
    type,
    updated_at,
    batch_id,
    batch_last_run
  FROM advertising_campaigns
), _advertising_keywords AS (
  SELECT
    ad_group_id,
    created_at,
    deleted_at,
    id,
    keyword_id,
    match_type,
    state,
    text,
    updated_at,
    batch_id,
    batch_last_run
  FROM advertising_keywords
), _advertising_targets AS (
  SELECT
    ad_group_id,
    created_at,
    deleted_at,
    id,
    target_id,
    text,
    updated_at,
    batch_id,
    batch_last_run
  FROM advertising_targets
), _Brands AS (
  SELECT
    amazon_seller_id,
    baseline_date,
    client_id,
    created_at,
    created_by,
    currency,
    deleted_at,
    deleted_by,
    enabled_executive_report,
    enabled_projections,
    estimated_anual_sales,
    growth_target,
    id,
    legacy_brand_id,
    map_formula,
    name,
    start_date,
    updated_at,
    updated_by,
    batch_id,
    batch_last_run
  FROM Brands
), _clients AS (
  SELECT
    advertisement_document_url,
    advertisement_enabled,
    amazon_seller_id,
    avatar,
    created_at,
    created_by,
    deleted_at,
    deleted_by,
    description,
    estimated_annual_sales,
    export_view_enabled,
    id,
    insights_view_enabled,
    map_view_enabled,
    member_since,
    name,
    ss_invoicing_view_enabled,
    ss_subscriptions_view_enabled,
    updated_at,
    updated_by,
    website,
    batch_id,
    batch_last_run
  FROM clients
), _advertising_portfolios AS (
  SELECT
    id,
    portfolio_id,
    name,
    budget_amount,
    budget_currency_code,
    budget_policy,
    in_budget,
    state,
    created_at,
    updated_at,
    deleted_at,
    opc,
    brand_id,
    use_dsp_budget,
    profile_id,
    marketplace_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    portfolio_client_id,
    portfoliobrandname,
    batch_id,
    batch_last_run
  FROM advertising_portfolios
), _portfolio_opcs AS (
  SELECT
    amount,
    created_at,
    created_by,
    id,
    month,
    portfolio_id,
    updated_at,
    year,
    batch_id,
    batch_last_run
  FROM portfolio_opcs
), _advertising_campaign_performance AS (
  SELECT
    campaign_budget_amount,
    campaign_budget_currency_code,
    campaign_id,
    campaign_name,
    campaign_status,
    clicks,
    cost,
    created_at,
    date,
    id,
    impressions,
    top_of_search_impression_share,
    updated_at,
    batch_id,
    batch_last_run
  FROM advertising_campaign_performance
), _Advertising_Walmart_Metrics_POC AS (
  SELECT
    id,
    date,
    customerid,
    customername,
    brandid,
    brandname,
    portfolioid,
    portfolioname,
    campaignid,
    campaignname,
    adgroupid,
    adgroupname,
    impressions,
    clicks,
    cost,
    revenue,
    orders,
    keywordid,
    keywordtext,
    brand_id
  FROM Advertising_Walmart_Metrics_POC
), _View_of_google_ads_campaings_by_day AS (
  SELECT
    requestid,
    customer_resourcename,
    customer_id,
    campaign_resourcename,
    campaign_status,
    campaign_basecampaign,
    campaign_name,
    campaign_id,
    campaign_campaignbudget,
    campaign_startdate,
    campaign_enddate,
    metrics_clicks,
    metrics_videoviews,
    metrics_conversions,
    metrics_ctr,
    metrics_activeviewimpressions,
    metrics_averagecost,
    metrics_averagecpc,
    metrics_averagecpm,
    metrics_impressions,
    metrics_interactions,
    date,
    segments_month,
    segments_year,
    client_customer_id,
    metrics_averagepageviews,
    metrics_averagetimeonsite,
    metrics_bouncerate,
    metrics_conversionsbyconversiondate,
    metrics_activeviewcpm,
    metrics_activeviewctr,
    google_ads_customer,
    campaign_labels,
    metrics_costmicros,
    metrics_conversionsvalue,
    campaign_biddingstrategy,
    biddingstrategy_resourcename,
    campaign_labels_text,
    queryresourceconsumption,
    batch_id,
    batch_last_run,
    portfolioname,
    google_ads_portfolio
  FROM View_of_google_ads_campaings_by_day
), _Ignore_deleted_at AS (
  SELECT
    *
  FROM _advertising_metrics
  WHERE
    (
      metric_deleted_at IS NULL AND date >= '2010-01-01'
    )
), _campaign_performance_campaigns AS (
  SELECT
    campaign_id,
    date,
    top_of_search_impression_share
  FROM _advertising_campaign_performance
), _Walmart_Formulas AS (
  SELECT
    date,
    customerid,
    customername,
    brandid,
    brandname,
    portfolioid,
    portfolioname,
    campaignid,
    campaignname,
    adgroupid,
    adgroupname,
    impressions,
    clicks,
    cost,
    revenue,
    orders,
    keywordid,
    keywordtext,
    brand_id,
    (
      'Walmart'
    ) AS category,
    (
      CONCAT('w', id)
    ) AS id
  FROM _Advertising_Walmart_Metrics_POC
), _Add_Formula_3 AS (
  SELECT
    requestid,
    customer_resourcename,
    customer_id,
    campaign_resourcename,
    campaign_status,
    campaign_basecampaign,
    campaign_name,
    campaign_id,
    campaign_campaignbudget,
    campaign_startdate,
    campaign_enddate,
    metrics_clicks,
    metrics_videoviews,
    metrics_conversions,
    metrics_ctr,
    metrics_activeviewimpressions,
    metrics_averagecost,
    metrics_averagecpc,
    metrics_averagecpm,
    metrics_impressions,
    metrics_interactions,
    date,
    segments_month,
    segments_year,
    client_customer_id,
    metrics_averagepageviews,
    metrics_averagetimeonsite,
    metrics_bouncerate,
    metrics_conversionsbyconversiondate,
    metrics_activeviewcpm,
    metrics_activeviewctr,
    google_ads_customer,
    campaign_labels,
    metrics_costmicros,
    metrics_conversionsvalue,
    campaign_biddingstrategy,
    biddingstrategy_resourcename,
    campaign_labels_text,
    queryresourceconsumption,
    batch_id,
    batch_last_run,
    portfolioname,
    google_ads_portfolio,
    (
      metrics_costmicros / NULLIF(1000000, 0)
    ) AS cost,
    (
      'GoogleAds'
    ) AS category,
    (
      CONCAT('g', campaign_id)
    ) AS id
  FROM _View_of_google_ads_campaings_by_day
), _Join_Data AS (
  SELECT
    _Ignore_deleted_at.ad_group_id,
    _Ignore_deleted_at.ad_group_name,
    _Ignore_deleted_at.attributed_conversions_14d,
    _Ignore_deleted_at.attributed_conversions_14d_same_sku,
    _Ignore_deleted_at.attributed_conversions_7d,
    _Ignore_deleted_at.attributed_conversions_7d_same_sku,
    _Ignore_deleted_at.attributed_detail_page_views_clicks_14d,
    _Ignore_deleted_at.attributed_order_rate_new_to_brand_14d,
    _Ignore_deleted_at.attributed_orders_new_to_brand_14d,
    _Ignore_deleted_at.attributed_orders_new_to_brand_percentage_14d,
    _Ignore_deleted_at.attributed_sales_14d,
    _Ignore_deleted_at.attributed_sales_14d_same_sku,
    _Ignore_deleted_at.attributed_sales_7d,
    _Ignore_deleted_at.attributed_sales_7d_same_sku,
    _Ignore_deleted_at.attributed_sales_new_to_brand_14d,
    _Ignore_deleted_at.attributed_sales_new_to_brand_percentage_14d,
    _Ignore_deleted_at.attributed_units_ordered_7d,
    _Ignore_deleted_at.attributed_units_ordered_7d_same_sku,
    _Ignore_deleted_at.attributed_units_ordered_new_to_brand_14d,
    _Ignore_deleted_at.attributed_units_ordered_new_to_brand_percentage_14d,
    _Ignore_deleted_at.campaign_id,
    _Ignore_deleted_at.campaign_name,
    _Ignore_deleted_at.clicks,
    _Ignore_deleted_at.cost,
    _Ignore_deleted_at.created_at,
    _Ignore_deleted_at.date,
    _Ignore_deleted_at.dpv_14d,
    _Ignore_deleted_at.external_id,
    _Ignore_deleted_at.external_name,
    _Ignore_deleted_at.id,
    _Ignore_deleted_at.impressions,
    _Ignore_deleted_at.portfolio_id,
    _Ignore_deleted_at.table,
    _Ignore_deleted_at.units_sold_14d,
    _Ignore_deleted_at.updated_at,
    _Ignore_deleted_at.dsp_total_units_sold,
    _Ignore_deleted_at.dsp_total_purchases,
    _Ignore_deleted_at.dsp_total_product_sales,
    _Ignore_deleted_at.ad_type,
    _Ignore_deleted_at.metric_deleted_at,
    _Ignore_deleted_at.batch_id,
    _Ignore_deleted_at.batch_last_run,
    _advertising_keywords.keyword_id,
    _advertising_keywords.match_type AS advertising_keywords_match_type,
    _advertising_keywords.text AS advertising_keywords_text
  FROM _Ignore_deleted_at
  LEFT JOIN _advertising_keywords
    ON _Ignore_deleted_at.external_id = _advertising_keywords.keyword_id
), _Group_By AS (
  SELECT
    campaign_id,
    date,
    MIN(top_of_search_impression_share) AS top_of_search_impression_share
  FROM _campaign_performance_campaigns
  GROUP BY
    campaign_id,
    date
), _Google_Ads_Columns AS (
  SELECT
    portfolioname,
    cost,
    metrics_impressions AS impressions,
    metrics_clicks AS clicks,
    campaign_id AS campaignid,
    date,
    campaign_name AS campaignname,
    metrics_conversions AS orders,
    category
  FROM _Add_Formula_3
), _Join_Data_1 AS (
  SELECT
    _Join_Data.ad_group_id,
    _Join_Data.created_at,
    _Join_Data.deleted_at,
    _Join_Data.id,
    _Join_Data.keyword_id,
    _Join_Data.match_type,
    _Join_Data.state,
    _Join_Data.text,
    _Join_Data.updated_at,
    _Join_Data.batch_id,
    _Join_Data.batch_last_run,
    _Join_Data.ad_group_name,
    _Join_Data.attributed_conversions_14d,
    _Join_Data.attributed_conversions_14d_same_sku,
    _Join_Data.attributed_conversions_7d,
    _Join_Data.attributed_conversions_7d_same_sku,
    _Join_Data.attributed_detail_page_views_clicks_14d,
    _Join_Data.attributed_order_rate_new_to_brand_14d,
    _Join_Data.attributed_orders_new_to_brand_14d,
    _Join_Data.attributed_orders_new_to_brand_percentage_14d,
    _Join_Data.attributed_sales_14d,
    _Join_Data.attributed_sales_14d_same_sku,
    _Join_Data.attributed_sales_7d,
    _Join_Data.attributed_sales_7d_same_sku,
    _Join_Data.attributed_sales_new_to_brand_14d,
    _Join_Data.attributed_sales_new_to_brand_percentage_14d,
    _Join_Data.attributed_units_ordered_7d,
    _Join_Data.attributed_units_ordered_7d_same_sku,
    _Join_Data.attributed_units_ordered_new_to_brand_14d,
    _Join_Data.attributed_units_ordered_new_to_brand_percentage_14d,
    _Join_Data.campaign_id,
    _Join_Data.campaign_name,
    _Join_Data.clicks,
    _Join_Data.cost,
    _Join_Data.date,
    _Join_Data.dpv_14d,
    _Join_Data.external_id,
    _Join_Data.external_name,
    _Join_Data.impressions,
    _Join_Data.portfolio_id,
    _Join_Data.table,
    _Join_Data.units_sold_14d,
    _Join_Data.dsp_total_units_sold,
    _Join_Data.dsp_total_purchases,
    _Join_Data.dsp_total_product_sales,
    _Join_Data.ad_type,
    _Join_Data.metric_deleted_at,
    _advertising_targets.target_id,
    _advertising_targets.text AS advertising_targets_text
  FROM _Join_Data
  LEFT JOIN _advertising_targets
    ON _Join_Data.external_id = _advertising_targets.target_id
), _Group_GA AS (
  SELECT
    date,
    portfolioname,
    campaignid,
    campaignname,
    SUM(cost) AS cost,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(orders) AS orders,
    MIN(category) AS category
  FROM _Google_Ads_Columns
  GROUP BY
    date,
    portfolioname,
    campaignid,
    campaignname
), _Join_Data_2 AS (
  SELECT
    _Join_Data_1.ad_group_id,
    _Join_Data_1.created_at,
    _Join_Data_1.deleted_at,
    _Join_Data_1.id,
    _Join_Data_1.keyword_id,
    _Join_Data_1.match_type,
    _Join_Data_1.state,
    _Join_Data_1.text,
    _Join_Data_1.updated_at,
    _Join_Data_1.batch_id,
    _Join_Data_1.batch_last_run,
    _Join_Data_1.ad_group_name,
    _Join_Data_1.attributed_conversions_14d,
    _Join_Data_1.attributed_conversions_14d_same_sku,
    _Join_Data_1.attributed_conversions_7d,
    _Join_Data_1.attributed_conversions_7d_same_sku,
    _Join_Data_1.attributed_detail_page_views_clicks_14d,
    _Join_Data_1.attributed_order_rate_new_to_brand_14d,
    _Join_Data_1.attributed_orders_new_to_brand_14d,
    _Join_Data_1.attributed_orders_new_to_brand_percentage_14d,
    _Join_Data_1.attributed_sales_14d,
    _Join_Data_1.attributed_sales_14d_same_sku,
    _Join_Data_1.attributed_sales_7d,
    _Join_Data_1.attributed_sales_7d_same_sku,
    _Join_Data_1.attributed_sales_new_to_brand_14d,
    _Join_Data_1.attributed_sales_new_to_brand_percentage_14d,
    _Join_Data_1.attributed_units_ordered_7d,
    _Join_Data_1.attributed_units_ordered_7d_same_sku,
    _Join_Data_1.attributed_units_ordered_new_to_brand_14d,
    _Join_Data_1.attributed_units_ordered_new_to_brand_percentage_14d,
    _Join_Data_1.campaign_id,
    _Join_Data_1.campaign_name,
    _Join_Data_1.clicks,
    _Join_Data_1.cost,
    _Join_Data_1.date,
    _Join_Data_1.dpv_14d,
    _Join_Data_1.external_id,
    _Join_Data_1.external_name,
    _Join_Data_1.impressions,
    _Join_Data_1.portfolio_id,
    _Join_Data_1.table,
    _Join_Data_1.units_sold_14d,
    _Join_Data_1.dsp_total_units_sold,
    _Join_Data_1.dsp_total_purchases,
    _Join_Data_1.dsp_total_product_sales,
    _Join_Data_1.ad_type,
    _Join_Data_1.metric_deleted_at,
    _Join_Data_1.target_id,
    _Join_Data_1.advertising_targets_text,
    _advertising_campaigns.category AS advertising_campaigns_category,
    _advertising_campaigns.name AS advertising_campaigns_name,
    _advertising_campaigns.promotion,
    _advertising_campaigns.targeting_type AS advertising_campaigns_targeting_type,
    _advertising_campaigns.type AS advertising_campaigns_type
  FROM _Join_Data_1
  LEFT JOIN _advertising_campaigns
    ON _Join_Data_1.campaign_id = _advertising_campaigns.campaign_id
), _Join_Data_3 AS (
  SELECT
    _Join_Data_2.ad_group_id,
    _Join_Data_2.created_at,
    _Join_Data_2.deleted_at,
    _Join_Data_2.id,
    _Join_Data_2.keyword_id,
    _Join_Data_2.match_type,
    _Join_Data_2.state,
    _Join_Data_2.text,
    _Join_Data_2.updated_at,
    _Join_Data_2.batch_id,
    _Join_Data_2.batch_last_run,
    _Join_Data_2.ad_group_name,
    _Join_Data_2.attributed_conversions_14d,
    _Join_Data_2.attributed_conversions_14d_same_sku,
    _Join_Data_2.attributed_conversions_7d,
    _Join_Data_2.attributed_conversions_7d_same_sku,
    _Join_Data_2.attributed_detail_page_views_clicks_14d,
    _Join_Data_2.attributed_order_rate_new_to_brand_14d,
    _Join_Data_2.attributed_orders_new_to_brand_14d,
    _Join_Data_2.attributed_orders_new_to_brand_percentage_14d,
    _Join_Data_2.attributed_sales_14d,
    _Join_Data_2.attributed_sales_14d_same_sku,
    _Join_Data_2.attributed_sales_7d,
    _Join_Data_2.attributed_sales_7d_same_sku,
    _Join_Data_2.attributed_sales_new_to_brand_14d,
    _Join_Data_2.attributed_sales_new_to_brand_percentage_14d,
    _Join_Data_2.attributed_units_ordered_7d,
    _Join_Data_2.attributed_units_ordered_7d_same_sku,
    _Join_Data_2.attributed_units_ordered_new_to_brand_14d,
    _Join_Data_2.attributed_units_ordered_new_to_brand_percentage_14d,
    _Join_Data_2.campaign_id,
    _Join_Data_2.campaign_name,
    _Join_Data_2.clicks,
    _Join_Data_2.cost,
    _Join_Data_2.date,
    _Join_Data_2.dpv_14d,
    _Join_Data_2.external_id,
    _Join_Data_2.external_name,
    _Join_Data_2.impressions,
    _Join_Data_2.portfolio_id,
    _Join_Data_2.table,
    _Join_Data_2.units_sold_14d,
    _Join_Data_2.dsp_total_units_sold,
    _Join_Data_2.dsp_total_purchases,
    _Join_Data_2.dsp_total_product_sales,
    _Join_Data_2.ad_type,
    _Join_Data_2.metric_deleted_at,
    _Join_Data_2.target_id,
    _Join_Data_2.advertising_targets_text,
    _Join_Data_2.advertising_campaigns_category,
    _Join_Data_2.advertising_campaigns_name,
    _Join_Data_2.promotion,
    _Join_Data_2.advertising_campaigns_targeting_type,
    _Join_Data_2.advertising_campaigns_type,
    _advertising_portfolios.name AS advertising_portfolios_name,
    _advertising_portfolios.budget_currency_code,
    _advertising_portfolios.brand_id,
    _advertising_portfolios.use_dsp_budget,
    _advertising_portfolios.profile_id,
    _advertising_portfolios.marketplace_id,
    _advertising_portfolios.master_portfolio_id,
    _advertising_portfolios.master_portfolio_name,
    _advertising_portfolios.master_portfolio_brand_id,
    _advertising_portfolios.master_portfolio_use_dsp_budget,
    _advertising_portfolios.portfolio_client_id,
    _advertising_portfolios.portfoliobrandname
  FROM _Join_Data_2
  INNER JOIN _advertising_portfolios
    ON _Join_Data_2.portfolio_id = _advertising_portfolios.portfolio_id
), _Group_metrics_in_master_portfolios AS (
  SELECT
    ad_group_id,
    created_at,
    deleted_at,
    id,
    keyword_id,
    match_type,
    state,
    text,
    updated_at,
    batch_id,
    batch_last_run,
    ad_group_name,
    attributed_conversions_14d,
    attributed_conversions_14d_same_sku,
    attributed_conversions_7d,
    attributed_conversions_7d_same_sku,
    attributed_detail_page_views_clicks_14d,
    attributed_order_rate_new_to_brand_14d,
    attributed_orders_new_to_brand_14d,
    attributed_orders_new_to_brand_percentage_14d,
    attributed_sales_14d,
    attributed_sales_14d_same_sku,
    attributed_sales_7d,
    attributed_sales_7d_same_sku,
    attributed_sales_new_to_brand_14d,
    attributed_sales_new_to_brand_percentage_14d,
    attributed_units_ordered_7d,
    attributed_units_ordered_7d_same_sku,
    attributed_units_ordered_new_to_brand_14d,
    attributed_units_ordered_new_to_brand_percentage_14d,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    date,
    dpv_14d,
    external_id,
    external_name,
    impressions,
    table,
    units_sold_14d,
    dsp_total_units_sold,
    dsp_total_purchases,
    dsp_total_product_sales,
    ad_type,
    metric_deleted_at,
    target_id,
    advertising_targets_text,
    advertising_campaigns_category,
    advertising_campaigns_name,
    promotion,
    advertising_campaigns_targeting_type,
    advertising_campaigns_type,
    budget_currency_code,
    use_dsp_budget,
    profile_id,
    marketplace_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    portfolio_client_id,
    portfoliobrandname,
    (
      COALESCE(master_portfolio_id, portfolio_id)
    ) AS portfolio_id,
    (
      COALESCE(master_portfolio_name, advertising_portfolios_name)
    ) AS advertising_portfolios_name,
    (
      COALESCE(master_portfolio_brand_id, brand_id)
    ) AS brand_id
  FROM _Join_Data_3
), _Join_Data_4 AS (
  SELECT
    _Group_metrics_in_master_portfolios.ad_group_id,
    _Group_metrics_in_master_portfolios.created_at,
    _Group_metrics_in_master_portfolios.deleted_at,
    _Group_metrics_in_master_portfolios.id,
    _Group_metrics_in_master_portfolios.keyword_id,
    _Group_metrics_in_master_portfolios.match_type,
    _Group_metrics_in_master_portfolios.state,
    _Group_metrics_in_master_portfolios.text,
    _Group_metrics_in_master_portfolios.updated_at,
    _Group_metrics_in_master_portfolios.batch_id,
    _Group_metrics_in_master_portfolios.batch_last_run,
    _Group_metrics_in_master_portfolios.ad_group_name,
    _Group_metrics_in_master_portfolios.attributed_conversions_14d,
    _Group_metrics_in_master_portfolios.attributed_conversions_14d_same_sku,
    _Group_metrics_in_master_portfolios.attributed_conversions_7d,
    _Group_metrics_in_master_portfolios.attributed_conversions_7d_same_sku,
    _Group_metrics_in_master_portfolios.attributed_detail_page_views_clicks_14d,
    _Group_metrics_in_master_portfolios.attributed_order_rate_new_to_brand_14d,
    _Group_metrics_in_master_portfolios.attributed_orders_new_to_brand_14d,
    _Group_metrics_in_master_portfolios.attributed_orders_new_to_brand_percentage_14d,
    _Group_metrics_in_master_portfolios.attributed_sales_14d,
    _Group_metrics_in_master_portfolios.attributed_sales_14d_same_sku,
    _Group_metrics_in_master_portfolios.attributed_sales_7d,
    _Group_metrics_in_master_portfolios.attributed_sales_7d_same_sku,
    _Group_metrics_in_master_portfolios.attributed_sales_new_to_brand_14d,
    _Group_metrics_in_master_portfolios.attributed_sales_new_to_brand_percentage_14d,
    _Group_metrics_in_master_portfolios.attributed_units_ordered_7d,
    _Group_metrics_in_master_portfolios.attributed_units_ordered_7d_same_sku,
    _Group_metrics_in_master_portfolios.attributed_units_ordered_new_to_brand_14d,
    _Group_metrics_in_master_portfolios.attributed_units_ordered_new_to_brand_percentage_14d,
    _Group_metrics_in_master_portfolios.campaign_id,
    _Group_metrics_in_master_portfolios.campaign_name,
    _Group_metrics_in_master_portfolios.clicks,
    _Group_metrics_in_master_portfolios.cost,
    _Group_metrics_in_master_portfolios.date,
    _Group_metrics_in_master_portfolios.dpv_14d,
    _Group_metrics_in_master_portfolios.external_id,
    _Group_metrics_in_master_portfolios.external_name,
    _Group_metrics_in_master_portfolios.impressions,
    _Group_metrics_in_master_portfolios.portfolio_id,
    _Group_metrics_in_master_portfolios.table,
    _Group_metrics_in_master_portfolios.units_sold_14d,
    _Group_metrics_in_master_portfolios.dsp_total_units_sold,
    _Group_metrics_in_master_portfolios.dsp_total_purchases,
    _Group_metrics_in_master_portfolios.dsp_total_product_sales,
    _Group_metrics_in_master_portfolios.ad_type,
    _Group_metrics_in_master_portfolios.metric_deleted_at,
    _Group_metrics_in_master_portfolios.target_id,
    _Group_metrics_in_master_portfolios.advertising_targets_text,
    _Group_metrics_in_master_portfolios.advertising_campaigns_category,
    _Group_metrics_in_master_portfolios.advertising_campaigns_name,
    _Group_metrics_in_master_portfolios.promotion,
    _Group_metrics_in_master_portfolios.advertising_campaigns_targeting_type,
    _Group_metrics_in_master_portfolios.advertising_campaigns_type,
    _Group_metrics_in_master_portfolios.advertising_portfolios_name,
    _Group_metrics_in_master_portfolios.budget_currency_code,
    _Group_metrics_in_master_portfolios.brand_id,
    _Group_metrics_in_master_portfolios.use_dsp_budget,
    _Group_metrics_in_master_portfolios.profile_id,
    _Group_metrics_in_master_portfolios.marketplace_id,
    _Group_metrics_in_master_portfolios.master_portfolio_id,
    _Group_metrics_in_master_portfolios.master_portfolio_name,
    _Group_metrics_in_master_portfolios.master_portfolio_brand_id,
    _Group_metrics_in_master_portfolios.master_portfolio_use_dsp_budget,
    _Group_metrics_in_master_portfolios.portfolio_client_id,
    _Group_metrics_in_master_portfolios.portfoliobrandname,
    _Brands.amazon_seller_id,
    _Brands.client_id,
    _Brands.growth_target,
    _Brands.name AS brands_name
  FROM _Group_metrics_in_master_portfolios
  RIGHT JOIN _Brands
    ON _Group_metrics_in_master_portfolios.brand_id = _Brands.id
), _Distinct_Portfolios AS (
  SELECT
    portfolio_id,
    advertising_portfolios_name,
    MIN(brand_id) AS brandid
  FROM _Group_metrics_in_master_portfolios
  GROUP BY
    portfolio_id,
    advertising_portfolios_name
), _Join_Data_5 AS (
  SELECT
    _Join_Data_4.amazon_seller_id,
    _Join_Data_4.baseline_date,
    _Join_Data_4.client_id,
    _Join_Data_4.created_at,
    _Join_Data_4.created_by,
    _Join_Data_4.currency,
    _Join_Data_4.deleted_at,
    _Join_Data_4.deleted_by,
    _Join_Data_4.enabled_executive_report,
    _Join_Data_4.enabled_projections,
    _Join_Data_4.estimated_anual_sales,
    _Join_Data_4.growth_target,
    _Join_Data_4.id,
    _Join_Data_4.legacy_brand_id,
    _Join_Data_4.map_formula,
    _Join_Data_4.name,
    _Join_Data_4.start_date,
    _Join_Data_4.updated_at,
    _Join_Data_4.updated_by,
    _Join_Data_4.batch_id,
    _Join_Data_4.batch_last_run,
    _Join_Data_4.ad_group_id,
    _Join_Data_4.keyword_id,
    _Join_Data_4.match_type,
    _Join_Data_4.state,
    _Join_Data_4.text,
    _Join_Data_4.ad_group_name,
    _Join_Data_4.attributed_conversions_14d,
    _Join_Data_4.attributed_conversions_14d_same_sku,
    _Join_Data_4.attributed_conversions_7d,
    _Join_Data_4.attributed_conversions_7d_same_sku,
    _Join_Data_4.attributed_detail_page_views_clicks_14d,
    _Join_Data_4.attributed_order_rate_new_to_brand_14d,
    _Join_Data_4.attributed_orders_new_to_brand_14d,
    _Join_Data_4.attributed_orders_new_to_brand_percentage_14d,
    _Join_Data_4.attributed_sales_14d,
    _Join_Data_4.attributed_sales_14d_same_sku,
    _Join_Data_4.attributed_sales_7d,
    _Join_Data_4.attributed_sales_7d_same_sku,
    _Join_Data_4.attributed_sales_new_to_brand_14d,
    _Join_Data_4.attributed_sales_new_to_brand_percentage_14d,
    _Join_Data_4.attributed_units_ordered_7d,
    _Join_Data_4.attributed_units_ordered_7d_same_sku,
    _Join_Data_4.attributed_units_ordered_new_to_brand_14d,
    _Join_Data_4.attributed_units_ordered_new_to_brand_percentage_14d,
    _Join_Data_4.campaign_id,
    _Join_Data_4.campaign_name,
    _Join_Data_4.clicks,
    _Join_Data_4.cost,
    _Join_Data_4.date,
    _Join_Data_4.dpv_14d,
    _Join_Data_4.external_id,
    _Join_Data_4.external_name,
    _Join_Data_4.impressions,
    _Join_Data_4.portfolio_id,
    _Join_Data_4.table,
    _Join_Data_4.units_sold_14d,
    _Join_Data_4.dsp_total_units_sold,
    _Join_Data_4.dsp_total_purchases,
    _Join_Data_4.dsp_total_product_sales,
    _Join_Data_4.ad_type,
    _Join_Data_4.metric_deleted_at,
    _Join_Data_4.target_id,
    _Join_Data_4.advertising_targets_text,
    _Join_Data_4.advertising_campaigns_category,
    _Join_Data_4.advertising_campaigns_name,
    _Join_Data_4.promotion,
    _Join_Data_4.advertising_campaigns_targeting_type,
    _Join_Data_4.advertising_campaigns_type,
    _Join_Data_4.advertising_portfolios_name,
    _Join_Data_4.budget_currency_code,
    _Join_Data_4.brand_id,
    _Join_Data_4.use_dsp_budget,
    _Join_Data_4.profile_id,
    _Join_Data_4.marketplace_id,
    _Join_Data_4.master_portfolio_id,
    _Join_Data_4.master_portfolio_name,
    _Join_Data_4.master_portfolio_brand_id,
    _Join_Data_4.master_portfolio_use_dsp_budget,
    _Join_Data_4.portfolio_client_id,
    _Join_Data_4.portfoliobrandname,
    _clients.name AS clients_name
  FROM _Join_Data_4
  LEFT JOIN _clients
    ON _Join_Data_4.client_id = _clients.id
), _Join_Data_7 AS (
  SELECT
    _Group_GA.date,
    _Group_GA.portfolioname,
    _Group_GA.campaignid,
    _Group_GA.campaignname,
    _Group_GA.cost,
    _Group_GA.clicks,
    _Group_GA.impressions,
    _Group_GA.orders,
    _Group_GA.category,
    _Distinct_Portfolios.portfolio_id AS portfolioid,
    _Distinct_Portfolios.advertising_portfolios_name,
    _Distinct_Portfolios.brandid
  FROM _Group_GA
  INNER JOIN _Distinct_Portfolios
    ON _Group_GA.portfolioname = _Distinct_Portfolios.advertising_portfolios_name
), _Add_Formula AS (
  SELECT
    amazon_seller_id,
    baseline_date,
    client_id,
    created_at,
    created_by,
    currency,
    deleted_at,
    deleted_by,
    enabled_executive_report,
    enabled_projections,
    estimated_anual_sales,
    growth_target,
    id,
    legacy_brand_id,
    map_formula,
    name,
    start_date,
    updated_at,
    updated_by,
    batch_id,
    batch_last_run,
    ad_group_id,
    keyword_id,
    match_type,
    state,
    text,
    ad_group_name,
    attributed_conversions_14d,
    attributed_conversions_14d_same_sku,
    attributed_conversions_7d,
    attributed_conversions_7d_same_sku,
    attributed_detail_page_views_clicks_14d,
    attributed_order_rate_new_to_brand_14d,
    attributed_orders_new_to_brand_14d,
    attributed_orders_new_to_brand_percentage_14d,
    attributed_sales_14d,
    attributed_sales_14d_same_sku,
    attributed_sales_7d,
    attributed_sales_7d_same_sku,
    attributed_sales_new_to_brand_14d,
    attributed_sales_new_to_brand_percentage_14d,
    attributed_units_ordered_7d,
    attributed_units_ordered_7d_same_sku,
    attributed_units_ordered_new_to_brand_14d,
    attributed_units_ordered_new_to_brand_percentage_14d,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    date,
    dpv_14d,
    external_id,
    external_name,
    impressions,
    portfolio_id,
    table,
    units_sold_14d,
    dsp_total_units_sold,
    dsp_total_purchases,
    dsp_total_product_sales,
    ad_type,
    metric_deleted_at,
    target_id,
    advertising_targets_text,
    advertising_campaigns_category,
    advertising_campaigns_name,
    promotion,
    advertising_campaigns_targeting_type,
    advertising_campaigns_type,
    advertising_portfolios_name,
    budget_currency_code,
    brand_id,
    use_dsp_budget,
    profile_id,
    marketplace_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    portfolio_client_id,
    portfoliobrandname,
    clients_name,
    (
      YEAR(TO_DATE(date))
    ) AS year,
    (
      MONTH(TO_DATE(date))
    ) AS month,
    (
      CASE
        WHEN table = 'advertising_keywords'
        THEN advertising_keywords_match_type
        ELSE '-'
      END
    ) AS matchtype,
    (
      CASE
        WHEN campaign_name = 'Nest - STV Retargeting'
        THEN 'DSP'
        WHEN (
          campaign_name LIKE '%CNQ%'
        )
        THEN 'Non-Branded'
        WHEN (
          advertising_campaigns_category = 'branded'
        )
        THEN 'Branded'
        WHEN (
          advertising_campaigns_category = 'nonBranded'
        )
        THEN 'Non-Branded'
        WHEN (
          id LIKE 'd%'
        )
        THEN 'DSP'
        WHEN (
          advertising_campaigns_category = 'coupon'
        )
        THEN 'Coupon'
        WHEN (
          advertising_campaigns_category = 'lightning'
        )
        THEN 'Lightning Deal'
        WHEN (
          advertising_campaigns_category = 'percentageoff'
        )
        THEN 'PerCentOff'
        WHEN (
          advertising_campaigns_category = 'bogo'
        )
        THEN 'BOGO'
        WHEN (
          advertising_campaigns_category = 'socialmedia'
        )
        THEN 'SM'
        WHEN (
          advertising_campaigns_category = 'sscoupon'
        )
        THEN 'Subscribe & Save'
        WHEN (
          advertising_campaigns_category = 'creatorconnection'
        )
        THEN 'Creator Connection'
        WHEN (
          advertising_campaigns_category = 'reorderrewardscoupon'
        )
        THEN 'Reorder Rewards'
        ELSE advertising_campaigns_category
      END
    ) AS category,
    (
      CASE WHEN (
        clicks > 0
      ) THEN (
        cost / NULLIF(clicks, 0)
      ) ELSE 0 END
    ) AS cpc,
    (
      CASE
        WHEN (
          impressions > 0
        )
        THEN (
          clicks / NULLIF(impressions, 0)
        )
        ELSE 0
      END
    ) AS ctr,
    (
      CASE
        WHEN (
          attributed_sales_14d > attributed_sales_7d
        )
        THEN attributed_sales_14d
        ELSE attributed_sales_7d
      END
    ) AS revenue,
    (
      CASE
        WHEN (
          attributed_conversions_14d > attributed_conversions_7d
        )
        THEN attributed_conversions_14d
        ELSE attributed_conversions_7d
      END
    ) AS orders,
    (
      CASE
        WHEN (
          units_sold_14d > attributed_units_ordered_7d
        )
        THEN units_sold_14d
        ELSE attributed_units_ordered_7d
      END
    ) AS units,
    (
      COALESCE(
        advertising_campaigns_targeting_type,
        CASE
          WHEN (
            advertising_campaigns_targeting_type LIKE '-MANUAL-'
          )
          THEN 'MANUAL'
          WHEN (
            advertising_campaigns_targeting_type LIKE '-AUTO-'
          )
          THEN 'AI'
          ELSE 'MANUAL'
        END
      )
    ) AS targetingtype,
    (
      CASE
        WHEN (
          advertising_campaigns_category = 'branded'
        )
        THEN 'Search'
        WHEN (
          advertising_campaigns_category = 'nonBranded'
        )
        THEN 'Search'
        WHEN (
          id LIKE 'd%'
        )
        THEN 'DSP'
        WHEN (
          advertising_campaigns_category = 'coupon'
        )
        THEN 'Promo'
        WHEN (
          advertising_campaigns_category = 'lightning'
        )
        THEN 'Promo'
        WHEN (
          advertising_campaigns_category = 'percentageoff'
        )
        THEN 'Promo'
        WHEN (
          advertising_campaigns_category = 'bogo'
        )
        THEN 'Promo'
        WHEN (
          advertising_campaigns_category = 'socialmedia'
        )
        THEN 'Promo'
        WHEN (
          advertising_campaigns_category = 'sscoupon'
        )
        THEN 'Subscribe & Save'
        WHEN (
          advertising_campaigns_category = 'creatorconnection'
        )
        THEN 'Creator Connection'
        WHEN (
          advertising_campaigns_category = 'reorderrewardscoupon'
        )
        THEN 'Promo'
        ELSE advertising_campaigns_category
      END
    ) AS category_v2,
    (
      COALESCE(
        advertising_campaigns_targeting_type,
        CASE
          WHEN (
            advertising_campaigns_targeting_type LIKE '%MANUAL%'
          )
          THEN 'MANUAL'
          WHEN (
            advertising_campaigns_targeting_type LIKE '%AUTO%'
          )
          THEN 'AUTO (AI)'
          ELSE 'MANUAL'
        END
      )
    ) AS targetingmethod,
    (
      CASE
        WHEN (
          advertising_campaigns_category = 'branded'
        )
        THEN 'Branded'
        WHEN (
          advertising_campaigns_category = 'nonBranded'
        )
        THEN 'Non-Branded'
        WHEN (
          id LIKE 'd%'
        )
        THEN 'Audience Based'
      END
    ) AS targettingtype,
    (
      CASE
        WHEN advertising_targets_text LIKE '%category%'
        THEN 'Category'
        WHEN advertising_targets_text LIKE '%asin%'
        THEN 'ASIN'
        WHEN advertising_targets_text LIKE '%close-match%'
        THEN 'AI'
        WHEN advertising_targets_text LIKE '%loose-match%'
        THEN 'AI'
        WHEN advertising_targets_text LIKE '%substitutes%'
        THEN 'AI'
        WHEN advertising_targets_text LIKE '%compliments%'
        THEN 'AI'
        ELSE 'Keyword'
      END
    ) AS targetting_level,
    (
      CASE
        WHEN LOWER(category) = 'dsp'
        THEN CASE
          WHEN campaign_name LIKE '%Non-Purchase Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaign_name LIKE '%Purchase Retargeting%'
          THEN 'Purchase Retargeting'
          WHEN campaign_name LIKE '%- PRT%'
          THEN 'Purchase Retargeting'
          WHEN campaign_name LIKE '%- Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaign_name LIKE '%- Contextual%'
          THEN 'Category Targeting'
          WHEN campaign_name LIKE '%- IM%'
          THEN 'In Market'
          WHEN campaign_name LIKE '%- SPV%'
          THEN 'Similar Product View'
          WHEN campaign_name LIKE '%- VA%'
          THEN 'Similar Product View'
          WHEN campaign_name LIKE '%-PRT%'
          THEN 'Purchase Retargeting'
          WHEN campaign_name LIKE '%-Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaign_name LIKE '%-Contextual%'
          THEN 'Category Targeting'
          WHEN campaign_name LIKE '%-IM%'
          THEN 'In Market'
          WHEN campaign_name LIKE '%In-Market%'
          THEN 'In Market'
          WHEN campaign_name LIKE '%-SPV%'
          THEN 'Similar Product View'
          WHEN campaign_name LIKE '%Purchase Retargeting%'
          THEN 'Purchase Retargeting'
          WHEN campaign_name LIKE '%Non-Purchase%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaign_name LIKE '%In Market%'
          THEN 'In Market'
          WHEN campaign_name LIKE '%Category Targeting%'
          THEN 'Category Targeting'
          WHEN campaign_name LIKE '%Similar Product View%'
          THEN 'Similar Product View'
          ELSE CONCAT('Custom-', SUBSTRING_INDEX(SUBSTRING_INDEX(campaign_name, '-', 2), '-', -1))
        END
        WHEN campaign_name = 'Nest - STV Retargeting'
        THEN 'STV Retargeting'
        WHEN (
          advertising_campaigns_type = 'sp'
        )
        THEN 'Sponsored Product'
        WHEN (
          advertising_campaigns_type = 'sb'
        )
        THEN 'Sponsored Brand'
        WHEN (
          advertising_campaigns_type = 'sd'
        )
        THEN 'Sponsored Display'
        WHEN (
          advertising_campaigns_type = 'sbv'
        )
        THEN 'Sponsored Brand Video'
        WHEN (
          advertising_campaigns_type = ''
        )
        THEN 'Unknown'
        WHEN (
          advertising_campaigns_type = NULL
        )
        THEN 'Unknown'
        WHEN advertising_campaigns_name LIKE '%Lookalike%'
        THEN 'Lookalike'
        WHEN advertising_campaigns_name LIKE '%Reward Ads%'
        THEN 'Rewards'
        WHEN advertising_campaigns_name LIKE '%Retargeting - Q2%'
        THEN 'Spanish Language'
        WHEN advertising_campaigns_name LIKE '%Similar%'
        THEN 'Similar Product Views'
        WHEN advertising_campaigns_name LIKE '%market%'
        THEN 'In Market'
        WHEN advertising_campaigns_name LIKE '%Market%'
        THEN 'In Market'
        WHEN advertising_campaigns_name LIKE '%Contextual%'
        THEN 'Category Targeting'
        WHEN advertising_campaigns_name LIKE '%SPV%'
        THEN 'Similar Product Views'
        WHEN advertising_campaigns_name LIKE '%CTV%'
        THEN 'CTV'
        WHEN advertising_campaigns_name LIKE '%Video%'
        THEN 'Video'
        WHEN advertising_campaigns_name LIKE '%Retarget%'
        AND advertising_campaigns_type LIKE '%Loyalty'
        THEN 'Purchase Retargeting'
        WHEN advertising_campaigns_name LIKE '%Retarget%'
        THEN 'Non Purchase Retargeting'
        WHEN advertising_campaigns_name LIKE '%Mid Funnel%'
        THEN 'Non Purchase Retargeting'
        WHEN advertising_campaigns_name LIKE '%Lower Funnel%'
        THEN 'Non Purchase Retargeting'
        ELSE advertising_campaigns_type
      END
    ) AS adtype
  FROM _Join_Data_5
), _Add_Formula_1 AS (
  SELECT
    amazon_seller_id,
    baseline_date,
    client_id,
    created_at,
    created_by,
    currency,
    deleted_at,
    deleted_by,
    enabled_executive_report,
    enabled_projections,
    estimated_anual_sales,
    growth_target,
    id,
    legacy_brand_id,
    map_formula,
    name,
    start_date,
    updated_at,
    updated_by,
    batch_id,
    batch_last_run,
    ad_group_id,
    keyword_id,
    match_type,
    state,
    text,
    ad_group_name,
    attributed_conversions_14d,
    attributed_conversions_14d_same_sku,
    attributed_conversions_7d,
    attributed_conversions_7d_same_sku,
    attributed_detail_page_views_clicks_14d,
    attributed_order_rate_new_to_brand_14d,
    attributed_orders_new_to_brand_14d,
    attributed_orders_new_to_brand_percentage_14d,
    attributed_sales_14d,
    attributed_sales_14d_same_sku,
    attributed_sales_7d,
    attributed_sales_7d_same_sku,
    attributed_sales_new_to_brand_14d,
    attributed_sales_new_to_brand_percentage_14d,
    attributed_units_ordered_7d,
    attributed_units_ordered_7d_same_sku,
    attributed_units_ordered_new_to_brand_14d,
    attributed_units_ordered_new_to_brand_percentage_14d,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    date,
    dpv_14d,
    external_id,
    external_name,
    impressions,
    portfolio_id,
    table,
    units_sold_14d,
    dsp_total_units_sold,
    dsp_total_purchases,
    dsp_total_product_sales,
    ad_type,
    metric_deleted_at,
    target_id,
    advertising_targets_text,
    advertising_campaigns_category,
    advertising_campaigns_name,
    promotion,
    advertising_campaigns_targeting_type,
    advertising_campaigns_type,
    advertising_portfolios_name,
    budget_currency_code,
    brand_id,
    use_dsp_budget,
    profile_id,
    marketplace_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    portfolio_client_id,
    portfoliobrandname,
    clients_name,
    year,
    month,
    matchtype,
    category,
    adtype,
    cpc,
    ctr,
    revenue,
    orders,
    units,
    targetingtype,
    category_v2,
    targetingmethod,
    targettingtype,
    targetting_level,
    (
      CASE WHEN cost > 0 THEN revenue / NULLIF(cost, 0) ELSE 0 END
    ) AS roas,
    (
      CASE WHEN revenue THEN cost / NULLIF(revenue, 0) ELSE 0 END
    ) AS acos,
    (
      CASE WHEN orders THEN cost / NULLIF(orders, 0) ELSE 0 END
    ) AS costperconv,
    (
      CASE WHEN clicks > 0 THEN orders / NULLIF(clicks, 0) ELSE 0 END
    ) AS convrate,
    (
      CASE
        WHEN orders > 0
        THEN attributed_orders_new_to_brand_14d / NULLIF(orders, 0)
        ELSE 0
      END
    ) AS ntbordersperc,
    (
      CASE
        WHEN units > 0
        THEN attributed_units_ordered_new_to_brand_14d / NULLIF(units, 0)
        ELSE 0
      END
    ) AS ntbunitsperc,
    (
      CASE
        WHEN revenue > 0
        THEN attributed_sales_new_to_brand_14d / NULLIF(revenue, 0)
        ELSE 0
      END
    ) AS ntbsalesperc
  FROM _Add_Formula
), _Join_Data_6 AS (
  SELECT
    _Add_Formula_1.amazon_seller_id,
    _Add_Formula_1.baseline_date,
    _Add_Formula_1.client_id,
    _Add_Formula_1.created_at,
    _Add_Formula_1.created_by,
    _Add_Formula_1.currency,
    _Add_Formula_1.deleted_at,
    _Add_Formula_1.deleted_by,
    _Add_Formula_1.enabled_executive_report,
    _Add_Formula_1.enabled_projections,
    _Add_Formula_1.estimated_anual_sales,
    _Add_Formula_1.growth_target,
    _Add_Formula_1.id,
    _Add_Formula_1.legacy_brand_id,
    _Add_Formula_1.map_formula,
    _Add_Formula_1.name,
    _Add_Formula_1.start_date,
    _Add_Formula_1.updated_at,
    _Add_Formula_1.updated_by,
    _Add_Formula_1.batch_id,
    _Add_Formula_1.batch_last_run,
    _Add_Formula_1.ad_group_id,
    _Add_Formula_1.keyword_id,
    _Add_Formula_1.match_type,
    _Add_Formula_1.state,
    _Add_Formula_1.text,
    _Add_Formula_1.ad_group_name,
    _Add_Formula_1.attributed_conversions_14d,
    _Add_Formula_1.attributed_conversions_14d_same_sku,
    _Add_Formula_1.attributed_conversions_7d,
    _Add_Formula_1.attributed_conversions_7d_same_sku,
    _Add_Formula_1.attributed_detail_page_views_clicks_14d,
    _Add_Formula_1.attributed_order_rate_new_to_brand_14d,
    _Add_Formula_1.attributed_orders_new_to_brand_14d,
    _Add_Formula_1.attributed_orders_new_to_brand_percentage_14d,
    _Add_Formula_1.attributed_sales_14d,
    _Add_Formula_1.attributed_sales_14d_same_sku,
    _Add_Formula_1.attributed_sales_7d,
    _Add_Formula_1.attributed_sales_7d_same_sku,
    _Add_Formula_1.attributed_sales_new_to_brand_14d,
    _Add_Formula_1.attributed_sales_new_to_brand_percentage_14d,
    _Add_Formula_1.attributed_units_ordered_7d,
    _Add_Formula_1.attributed_units_ordered_7d_same_sku,
    _Add_Formula_1.attributed_units_ordered_new_to_brand_14d,
    _Add_Formula_1.attributed_units_ordered_new_to_brand_percentage_14d,
    _Add_Formula_1.campaign_id,
    _Add_Formula_1.campaign_name,
    _Add_Formula_1.clicks,
    _Add_Formula_1.cost,
    _Add_Formula_1.date,
    _Add_Formula_1.dpv_14d,
    _Add_Formula_1.external_id,
    _Add_Formula_1.external_name,
    _Add_Formula_1.impressions,
    _Add_Formula_1.portfolio_id,
    _Add_Formula_1.table,
    _Add_Formula_1.units_sold_14d,
    _Add_Formula_1.dsp_total_units_sold,
    _Add_Formula_1.dsp_total_purchases,
    _Add_Formula_1.dsp_total_product_sales,
    _Add_Formula_1.ad_type,
    _Add_Formula_1.metric_deleted_at,
    _Add_Formula_1.target_id,
    _Add_Formula_1.advertising_targets_text,
    _Add_Formula_1.advertising_campaigns_category,
    _Add_Formula_1.advertising_campaigns_name,
    _Add_Formula_1.promotion,
    _Add_Formula_1.advertising_campaigns_targeting_type,
    _Add_Formula_1.advertising_campaigns_type,
    _Add_Formula_1.advertising_portfolios_name,
    _Add_Formula_1.budget_currency_code,
    _Add_Formula_1.brand_id,
    _Add_Formula_1.use_dsp_budget,
    _Add_Formula_1.profile_id,
    _Add_Formula_1.marketplace_id,
    _Add_Formula_1.master_portfolio_id,
    _Add_Formula_1.master_portfolio_name,
    _Add_Formula_1.master_portfolio_brand_id,
    _Add_Formula_1.master_portfolio_use_dsp_budget,
    _Add_Formula_1.portfolio_client_id,
    _Add_Formula_1.portfoliobrandname,
    _Add_Formula_1.clients_name,
    _Add_Formula_1.year,
    _Add_Formula_1.month,
    _Add_Formula_1.matchtype,
    _Add_Formula_1.category,
    _Add_Formula_1.adtype,
    _Add_Formula_1.cpc,
    _Add_Formula_1.ctr,
    _Add_Formula_1.revenue,
    _Add_Formula_1.orders,
    _Add_Formula_1.units,
    _Add_Formula_1.targetingtype,
    _Add_Formula_1.category_v2,
    _Add_Formula_1.targetingmethod,
    _Add_Formula_1.targettingtype,
    _Add_Formula_1.targetting_level,
    _Add_Formula_1.roas,
    _Add_Formula_1.acos,
    _Add_Formula_1.costperconv,
    _Add_Formula_1.convrate,
    _Add_Formula_1.ntbordersperc,
    _Add_Formula_1.ntbunitsperc,
    _Add_Formula_1.ntbsalesperc,
    _portfolio_opcs.amount AS opc_amount
  FROM _Add_Formula_1
  LEFT JOIN _portfolio_opcs
    ON _Add_Formula_1.year = _portfolio_opcs.year
    AND _Add_Formula_1.month = _portfolio_opcs.month
    AND _Add_Formula_1.portfolio_id = _portfolio_opcs.portfolio_id
), _Add_Formula_2 AS (
  SELECT
    amazon_seller_id,
    baseline_date,
    client_id,
    created_at,
    created_by,
    currency,
    deleted_at,
    deleted_by,
    enabled_executive_report,
    enabled_projections,
    estimated_anual_sales,
    growth_target,
    id,
    legacy_brand_id,
    map_formula,
    name,
    start_date,
    updated_at,
    updated_by,
    batch_id,
    batch_last_run,
    ad_group_id,
    keyword_id,
    match_type,
    state,
    text,
    ad_group_name,
    attributed_conversions_14d,
    attributed_conversions_14d_same_sku,
    attributed_conversions_7d,
    attributed_conversions_7d_same_sku,
    attributed_detail_page_views_clicks_14d,
    attributed_order_rate_new_to_brand_14d,
    attributed_orders_new_to_brand_14d,
    attributed_orders_new_to_brand_percentage_14d,
    attributed_sales_14d,
    attributed_sales_14d_same_sku,
    attributed_sales_7d,
    attributed_sales_7d_same_sku,
    attributed_sales_new_to_brand_14d,
    attributed_sales_new_to_brand_percentage_14d,
    attributed_units_ordered_7d,
    attributed_units_ordered_7d_same_sku,
    attributed_units_ordered_new_to_brand_14d,
    attributed_units_ordered_new_to_brand_percentage_14d,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    date,
    dpv_14d,
    external_id,
    external_name,
    impressions,
    portfolio_id,
    table,
    units_sold_14d,
    dsp_total_units_sold,
    dsp_total_purchases,
    dsp_total_product_sales,
    ad_type,
    metric_deleted_at,
    target_id,
    advertising_targets_text,
    advertising_campaigns_category,
    advertising_campaigns_name,
    promotion,
    advertising_campaigns_targeting_type,
    advertising_campaigns_type,
    advertising_portfolios_name,
    budget_currency_code,
    brand_id,
    use_dsp_budget,
    profile_id,
    marketplace_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    portfolio_client_id,
    portfoliobrandname,
    clients_name,
    year,
    month,
    matchtype,
    category,
    adtype,
    cpc,
    ctr,
    revenue,
    orders,
    units,
    targetingtype,
    category_v2,
    targetingmethod,
    targettingtype,
    targetting_level,
    roas,
    acos,
    costperconv,
    convrate,
    ntbordersperc,
    ntbunitsperc,
    ntbsalesperc,
    opc_amount,
    (
      COALESCE(opc_amount, 0.00)
    ) AS opc,
    (
      CASE WHEN category = 'DSP' THEN (
        ad_type
      ) ELSE adtype END
    ) AS adtype_old,
    (
      CASE
        WHEN portfolio_id = '59291170771464'
        THEN CASE
          WHEN adtype = 'Sponsored Product'
          THEN CASE
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-cat%'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-comp%'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-cat'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%'
            THEN 'Generic'
            ELSE 'Branded'
          END
          WHEN adtype = 'Sponsored Brand Video'
          THEN CASE
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-cat%'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-comp%'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-cat'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%'
            THEN 'Generic'
            ELSE 'Branded-Video'
          END
          WHEN adtype = 'Sponsored Display'
          THEN CASE
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-cat%'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-comp%'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%' AND LOWER(campaign_name) LIKE '%-cat'
            THEN 'Competitive'
            WHEN LOWER(campaign_name) LIKE '%-nb-%'
            THEN 'Generic'
            ELSE 'Branded Sponsored Display'
          END
          ELSE adtype
        END
        ELSE CASE
          WHEN adtype = 'Sponsored Product'
          THEN CASE
            WHEN LOWER(campaign_name) LIKE '%-nonbr%'
            THEN 'Non-Branded'
            WHEN LOWER(campaign_name) LIKE '%-da%'
            THEN 'Non-Branded'
            WHEN LOWER(campaign_name) LIKE '%-nobr%'
            THEN 'Non-Branded'
            WHEN LOWER(campaign_name) LIKE '%-nb-%'
            THEN 'Non-Branded'
            ELSE 'Branded'
          END
          WHEN adtype = 'Sponsored Brand Video'
          THEN CASE
            WHEN LOWER(campaign_name) LIKE '%-non%'
            THEN 'Non-Branded-Video'
            WHEN LOWER(campaign_name) LIKE '%-nobr-%'
            THEN 'Non-Branded-Video'
            WHEN LOWER(campaign_name) LIKE '%-nb-%'
            THEN 'Non-Branded-Video'
            ELSE 'Branded-Video'
          END
          WHEN adtype = 'Sponsored Display'
          THEN CASE
            WHEN LOWER(campaign_name) LIKE '%-non%'
            THEN 'Non-Branded Sponsored Display'
            WHEN LOWER(campaign_name) LIKE '%-nobr-%'
            THEN 'Non-Branded Sponsored Display'
            WHEN LOWER(campaign_name) LIKE '%-nb-%'
            THEN 'Non-Branded Sponsored Display'
            ELSE 'Branded Sponsored Display'
          END
          ELSE adtype
        END
      END
    ) AS competitortactic,
    (
      CASE
        WHEN STR_CONTAINS('d', id)
        THEN COALESCE(advertising_campaigns_name, campaign_name)
        ELSE advertising_campaigns_name
      END
    ) AS campaignname
  FROM _Join_Data_6
), _New_Columns AS (
  SELECT
    amazon_seller_id,
    baseline_date,
    client_id,
    created_at,
    created_by,
    currency,
    deleted_at,
    deleted_by,
    enabled_executive_report,
    enabled_projections,
    estimated_anual_sales,
    growth_target,
    id,
    legacy_brand_id,
    map_formula,
    name,
    start_date,
    updated_at,
    updated_by,
    batch_id,
    batch_last_run,
    ad_group_id,
    keyword_id,
    match_type,
    state,
    text,
    ad_group_name,
    attributed_conversions_14d,
    attributed_conversions_14d_same_sku,
    attributed_conversions_7d,
    attributed_conversions_7d_same_sku,
    attributed_detail_page_views_clicks_14d,
    attributed_order_rate_new_to_brand_14d,
    attributed_orders_new_to_brand_14d,
    attributed_orders_new_to_brand_percentage_14d,
    attributed_sales_14d,
    attributed_sales_14d_same_sku,
    attributed_sales_7d,
    attributed_sales_7d_same_sku,
    attributed_sales_new_to_brand_14d,
    attributed_sales_new_to_brand_percentage_14d,
    attributed_units_ordered_7d,
    attributed_units_ordered_7d_same_sku,
    attributed_units_ordered_new_to_brand_14d,
    attributed_units_ordered_new_to_brand_percentage_14d,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    date,
    dpv_14d,
    external_id,
    impressions,
    portfolio_id,
    table,
    units_sold_14d,
    dsp_total_units_sold,
    dsp_total_purchases,
    dsp_total_product_sales,
    ad_type,
    metric_deleted_at,
    target_id,
    advertising_targets_text,
    advertising_campaigns_category,
    advertising_campaigns_name,
    advertising_campaigns_targeting_type,
    advertising_campaigns_type,
    advertising_portfolios_name,
    budget_currency_code,
    brand_id,
    use_dsp_budget,
    profile_id,
    marketplace_id,
    master_portfolio_id,
    master_portfolio_name,
    master_portfolio_brand_id,
    master_portfolio_use_dsp_budget,
    portfolio_client_id,
    portfoliobrandname,
    clients_name,
    year,
    month,
    matchtype,
    category,
    adtype,
    cpc,
    ctr,
    revenue,
    orders,
    units,
    targetingtype,
    category_v2,
    targetingmethod,
    targettingtype,
    targetting_level,
    roas,
    acos,
    costperconv,
    convrate,
    ntbordersperc,
    ntbunitsperc,
    ntbsalesperc,
    opc_amount,
    opc,
    adtype_old,
    competitortactic,
    campaignname,
    (
      CASE
        WHEN adtype = 'Sponsored Brand'
        THEN 'Paid Search'
        WHEN adtype = 'Sponsored Product'
        THEN 'Paid Search'
        WHEN adtype = 'Sponsored Brand Video'
        THEN 'Paid Search'
        WHEN adtype = 'Sponsored Display'
        THEN 'Programmatic Display'
        WHEN category = 'BOGO'
        THEN 'Promo'
        WHEN category = 'Lightning Deal'
        THEN 'Promo'
        WHEN category = 'PerCentOff'
        THEN 'Promo'
        WHEN category = 'SM'
        THEN 'Promo'
        WHEN category = 'DOTD'
        THEN 'Promo'
        WHEN category = 'Subscribe & Save'
        THEN 'S&S'
        WHEN campaign_id LIKE 'dsp-%' AND campaign_name LIKE '%STV%'
        THEN 'Programmatic Video'
        WHEN campaign_id LIKE 'dsp-%' AND campaign_name LIKE '%OLV%'
        THEN 'Programmatic Video'
        WHEN campaign_id LIKE 'dsp-%' AND LOWER(campaign_name) LIKE '%video%'
        THEN 'Programmatic Video'
        WHEN campaign_id LIKE 'dsp-%'
        THEN 'Programmatic Display'
        ELSE NULL
      END
    ) AS category_-_new,
    (
      CASE
        WHEN campaign_name LIKE '%-CORE'
        THEN 'CORE'
        WHEN campaign_name LIKE '%-SEO'
        THEN 'SEO'
        WHEN campaign_name LIKE '%-LAUNCH'
        THEN 'LAUNCH'
        WHEN campaign_name LIKE '%-TEST'
        THEN 'TEST'
        WHEN campaign_name LIKE '%-CORE-SEO'
        THEN 'CORE'
        WHEN campaign_name LIKE '%-CORE-LAUNCH'
        THEN 'CORE'
        WHEN campaign_name LIKE '%-CORE-TEST'
        THEN 'CORE'
        ELSE 'CORE'
      END
    ) AS campaign_label_-_tactic,
    (
      CASE
        WHEN promotion = 'STNDC'
        THEN 'Standard Coupon'
        WHEN promotion = 'SMPC'
        THEN 'Social Media Promo Code'
        WHEN promotion = 'SNSC'
        THEN 'SNS Coupon'
        WHEN promotion = 'BTP'
        THEN 'Brand Tailored Promotion'
        WHEN promotion = 'BOGO'
        THEN 'Buy One Get One'
        WHEN promotion = 'PCTOFF'
        THEN 'Percentage Off'
        WHEN promotion = 'LD'
        THEN 'Lightning Deal'
        WHEN promotion = 'ROC'
        THEN 'Reorder Coupon'
        WHEN promotion = 'BD'
        THEN 'Best Deal'
        WHEN promotion = 'TD'
        THEN 'Top Deal'
        ELSE ''
      END
    ) AS promotion,
    (
      CASE
        WHEN advertising_campaigns_type = 'sd'
        THEN 'DISPLAY'
        WHEN campaign_id LIKE '%dsp%'
        THEN 'DSP'
        WHEN LOWER(campaignname) LIKE '%| pat%'
        OR LOWER(campaignname) LIKE '%-pat%'
        OR LOWER(campaignname) LIKE '%pat-%'
        OR LOWER(campaignname) LIKE '% pat'
        OR LOWER(campaignname) LIKE '%pat %'
        THEN 'CATEGORY'
        WHEN LOWER(campaignname) LIKE '%| kw%'
        OR LOWER(campaignname) LIKE '%-kw%'
        OR LOWER(campaignname) LIKE '%kw-%'
        OR LOWER(campaignname) LIKE '% kw'
        OR LOWER(campaignname) LIKE '%kw %'
        OR LOWER(campaignname) LIKE '%|kw%'
        THEN 'KEYWORD'
        WHEN LOWER(campaignname) LIKE '%-mixed%' OR LOWER(campaignname) LIKE '%_mixed%'
        THEN 'KEYWORD'
        WHEN LOWER(campaignname) LIKE '%-asin%' OR LOWER(campaignname) LIKE '%_asin%'
        THEN 'CATEGORY'
        WHEN LOWER(campaignname) LIKE '%-auto%' OR LOWER(campaignname) LIKE '%_auto%'
        THEN 'AUTO'
        WHEN LOWER(campaignname) LIKE '%category%'
        OR LOWER(campaignname) LIKE '%-cat%'
        OR LOWER(campaignname) LIKE '%| cat%'
        THEN 'CATEGORY'
        WHEN LOWER(campaignname) LIKE '%-broad%'
        OR LOWER(campaignname) LIKE '%-phrase%'
        OR LOWER(campaignname) LIKE '%-exact%'
        OR LOWER(campaignname) LIKE '%bmm%'
        THEN 'KEYWORD'
        ELSE 'Unknown'
      END
    ) AS advertising_tactic,
    (
      CASE
        WHEN table = 'advertising_keywords'
        THEN advertising_keywords_text
        WHEN table = 'advertising_targets'
        THEN advertising_targets_text
        ELSE external_name
      END
    ) AS external_name,
    (
      CASE
        WHEN campaign_name = 'Nest - STV Retargeting'
        THEN 'DSP'
        WHEN advertising_campaigns_name LIKE '%Retargeting - Q2%'
        THEN 'Spanish Language'
        WHEN adtype = 'Sponsored Product'
        THEN 'Sponsored Product'
        WHEN adtype = 'Sponsored Brand'
        THEN 'Sponsored Brand'
        WHEN adtype = 'Sponsored Brand Video'
        THEN 'Sponsored Brand Video'
        WHEN advertising_campaigns_type = 'sd'
        THEN 'Sponsored Display'
        WHEN campaign_id LIKE '%dsp-%' AND LOWER(campaign_name) LIKE '%ctv%'
        THEN 'Streaming TV'
        WHEN campaign_id LIKE '%dsp-%' AND LOWER(campaign_name) LIKE '%olv%'
        THEN 'Online Video'
        WHEN campaign_id LIKE '%dsp-%' AND LOWER(campaign_name) LIKE '%video%'
        THEN 'Online Video'
        WHEN adtype = 'BOGO'
        THEN 'BOGO'
        WHEN category = 'Lightning Deal'
        THEN 'Lightning Deal'
        WHEN category = 'PerCentOff'
        THEN 'PerCentOff'
        WHEN category = 'DOTD'
        THEN 'Promo'
        WHEN category_-_new = 'S&S'
        THEN 'Subscribe & Save'
        WHEN campaign_id LIKE '%dsp-%'
        THEN 'DSP'
      END
    ) AS ad_type_-_new,
    (
      CASE
        WHEN ad_type_-_new = 'Sponsored Product'
        THEN 'SP'
        WHEN ad_type_-_new = 'Sponsored Brand'
        THEN 'SB'
        WHEN ad_type_-_new = 'Sponsored Brand Video'
        THEN 'SB'
        WHEN ad_type_-_new = 'Sponsored Display'
        THEN 'SD'
        WHEN ad_type_-_new = 'DSP'
        THEN CASE
          WHEN campaignname LIKE '%-PRT%'
          THEN 'PR'
          WHEN campaignname LIKE '%-Retargeting%'
          THEN 'NPR'
          WHEN campaignname LIKE '%-Contextual%'
          THEN 'CT'
          WHEN campaignname LIKE '%-IM-%'
          THEN 'IM'
          WHEN campaignname LIKE '%-SPV-%'
          THEN 'SPV'
          ELSE 'CUSTOM'
        END
        WHEN ad_type_-_new IN ('Streaming TV', 'Online Video')
        THEN CASE
          WHEN campaignname LIKE '%-PRT%'
          THEN 'PR'
          WHEN campaignname LIKE '%-Retargeting%'
          THEN 'NPR'
          WHEN campaignname LIKE '%-Contextual%'
          THEN 'CT'
          WHEN campaignname LIKE '%-IM-%'
          THEN 'IM'
          WHEN campaignname LIKE '%-SPV-%'
          THEN 'SPV'
          ELSE 'General'
        END
        ELSE ''
      END
    ) AS short_name,
    (
      CASE
        WHEN campaignname = 'Nest - STV Retargeting'
        THEN 'STV Retargeting'
        WHEN campaignname LIKE '%Reward Ads%'
        THEN 'Rewards'
        WHEN campaignname LIKE '%Lookalike%'
        THEN 'Lookalike'
        WHEN campaignname LIKE '%Retargeting - Q2%'
        THEN 'Non-Purchase Retargeting'
        WHEN campaignname LIKE '%-CNQ%'
        THEN 'Conquesting'
        WHEN ad_type_-_new = 'Sponsored Display'
        THEN CASE
          WHEN campaignname LIKE '%-PRT%'
          THEN 'Purchase Retargeting'
          WHEN campaignname LIKE '%-Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%-Contextual%'
          THEN 'Category Targeting'
          WHEN campaignname LIKE '%-IM-%'
          THEN 'In Market'
          WHEN campaignname LIKE '%-SPV-%'
          THEN 'Similar Product View'
          ELSE NULL
        END
        WHEN ad_type_-_new = 'DSP'
        THEN CASE
          WHEN campaignname LIKE '%Non-Purchase Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%Non Purchase Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%Purchase Retargeting%'
          THEN 'Purchase Retargeting'
          WHEN campaignname LIKE '%- PRT%'
          THEN 'Purchase Retargeting'
          WHEN campaignname LIKE '%- Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%- Contextual%'
          THEN 'Category Targeting'
          WHEN campaignname LIKE '%- IM%'
          THEN 'In Market'
          WHEN campaignname LIKE '%- SPV%'
          THEN 'Similar Product View'
          WHEN campaignname LIKE '%- VA%'
          THEN 'Similar Product View'
          WHEN campaignname LIKE '%-PRT%'
          THEN 'Purchase Retargeting'
          WHEN campaignname LIKE '%-Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%-Contextual%'
          THEN 'Category Targeting'
          WHEN campaignname LIKE '%-IM%'
          THEN 'In Market'
          WHEN campaignname LIKE '%In-Market%'
          THEN 'In Market'
          WHEN campaignname LIKE '%-SPV%'
          THEN 'Similar Product View'
          WHEN campaignname LIKE '%Purchase Retargeting%'
          THEN 'Purchase Retargeting'
          WHEN campaignname LIKE '%Non-Purchase%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%Non Purchase%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%In Market%'
          THEN 'In Market'
          WHEN campaignname LIKE '%Category Targeting%'
          THEN 'Category Targeting'
          WHEN campaignname LIKE '%Similar Product View%'
          THEN 'Similar Product View'
          ELSE CONCAT('Custom-', SUBSTRING_INDEX(SUBSTRING_INDEX(campaignname, '-', 2), '-', -1))
        END
        WHEN ad_type_-_new IN ('Streaming TV', 'Online Video')
        THEN CASE
          WHEN campaignname LIKE '%-PRT%'
          THEN 'Purchase Retargeting'
          WHEN campaignname LIKE '%-Retargeting%'
          THEN 'Non-Purchase Retargeting'
          WHEN campaignname LIKE '%-Contextual%'
          THEN 'Category Targeting'
          WHEN campaignname LIKE '%-IM-%'
          THEN 'In Market'
          WHEN campaignname LIKE '%-SPV-%'
          THEN 'Similar Product View'
          ELSE 'General'
        END
        WHEN ad_type_-_new IN ('Sponsored Product', 'Sponsored Brand', 'Sponsored Brand Video')
        THEN CASE
          WHEN campaignname LIKE '%-B-%'
          OR campaignname LIKE '%-Branded-%'
          OR campaignname LIKE '%| B |%'
          OR campaignname LIKE '%|B|%'
          OR campaignname LIKE '%|Branded|%'
          OR campaignname LIKE '%| Branded |%'
          THEN 'Branded'
          WHEN campaignname LIKE '%-NB-%'
          OR campaignname LIKE '%NonBranded%'
          OR campaignname LIKE '%Non-Branded%'
          OR campaignname LIKE '%|NB|%'
          OR campaignname LIKE '%| NB |%'
          THEN 'Non Branded'
          ELSE 'Branded'
        END
        ELSE NULL
      END
    ) AS targeting_type_-_new,
    (
      CASE
        WHEN campaignname = 'Nest - STV Retargeting'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'Lookalike'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'Rewards'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'Branded'
        THEN 'Brand Defense'
        WHEN targeting_type_-_new = 'Non Branded'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'Conquesting'
        THEN 'Conquesting'
        WHEN targeting_type_-_new = 'Purchase Retargeting'
        THEN 'Brand Defense'
        WHEN targeting_type_-_new = 'Non-Purchase Retargeting'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'Category Targeting'
        THEN 'Conquesting'
        WHEN targeting_type_-_new = 'In Market'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'Similar Product View'
        THEN 'Conquesting'
        WHEN targeting_type_-_new = 'Value Add'
        THEN 'Category Growth'
        WHEN targeting_type_-_new LIKE '%Custom%'
        THEN 'Category Growth'
        WHEN targeting_type_-_new = 'General'
        THEN 'Category Growth'
      END
    ) AS strategy_-_new,
    (
      CASE
        WHEN targetingmethod = 'AI'
        THEN 'AI'
        WHEN targeting_type_-_new = 'Value Add'
        THEN 'Value Add'
        WHEN ad_type_-_new IN ('DSP', 'Sponsored Display', 'Streaming TV', 'Online Video')
        THEN 'Audience Based'
        WHEN advertising_targets_text LIKE '%category%'
        THEN 'Category'
        WHEN advertising_targets_text LIKE '%asin%'
        THEN 'ASIN'
        ELSE 'Keyword'
      END
    ) AS targeting_level_-_new,
    (
      CASE
        WHEN targeting_level_-_new = 'ASIN'
        THEN 'ASIN'
        WHEN targeting_level_-_new = 'Category'
        THEN 'Cat'
        WHEN ad_type_-_new = 'Sponsored Display'
        THEN CASE
          WHEN targeting_type_-_new IN ('Purchase Retargeting', 'Non-Purchase Retargeting')
          THEN 'ASIN'
          WHEN targeting_type_-_new = 'Category Targeting'
          THEN 'Exact'
          WHEN targeting_type_-_new = 'In Market'
          THEN 'Broad'
          WHEN targeting_type_-_new = 'Similar Product View'
          THEN 'Phrase'
        END
        WHEN table = 'advertising_keywords'
        THEN CASE
          WHEN advertising_keywords_match_type = 'BROAD'
          THEN 'Broad'
          WHEN advertising_keywords_match_type = 'EXACT'
          THEN 'Exact'
          WHEN advertising_keywords_match_type = 'PHRASE'
          THEN 'Phrase'
        END
        ELSE ''
      END
    ) AS match_type_-_new
  FROM _Add_Formula_2
), _Joining_with_campaign_performance AS (
  SELECT
    _Group_By.top_of_search_impression_share,
    _New_Columns.amazon_seller_id,
    _New_Columns.baseline_date,
    _New_Columns.client_id,
    _New_Columns.created_at,
    _New_Columns.created_by,
    _New_Columns.currency,
    _New_Columns.deleted_at,
    _New_Columns.deleted_by,
    _New_Columns.enabled_executive_report,
    _New_Columns.enabled_projections,
    _New_Columns.estimated_anual_sales,
    _New_Columns.growth_target,
    _New_Columns.id,
    _New_Columns.legacy_brand_id,
    _New_Columns.map_formula,
    _New_Columns.name,
    _New_Columns.start_date,
    _New_Columns.updated_at,
    _New_Columns.updated_by,
    _New_Columns.batch_id,
    _New_Columns.batch_last_run,
    _New_Columns.ad_group_id,
    _New_Columns.keyword_id,
    _New_Columns.match_type,
    _New_Columns.state,
    _New_Columns.text,
    _New_Columns.ad_group_name,
    _New_Columns.attributed_conversions_14d,
    _New_Columns.attributed_conversions_14d_same_sku,
    _New_Columns.attributed_conversions_7d,
    _New_Columns.attributed_conversions_7d_same_sku,
    _New_Columns.attributed_detail_page_views_clicks_14d,
    _New_Columns.attributed_order_rate_new_to_brand_14d,
    _New_Columns.attributed_orders_new_to_brand_14d,
    _New_Columns.attributed_orders_new_to_brand_percentage_14d,
    _New_Columns.attributed_sales_14d,
    _New_Columns.attributed_sales_14d_same_sku,
    _New_Columns.attributed_sales_7d,
    _New_Columns.attributed_sales_7d_same_sku,
    _New_Columns.attributed_sales_new_to_brand_14d,
    _New_Columns.attributed_sales_new_to_brand_percentage_14d,
    _New_Columns.attributed_units_ordered_7d,
    _New_Columns.attributed_units_ordered_7d_same_sku,
    _New_Columns.attributed_units_ordered_new_to_brand_14d,
    _New_Columns.attributed_units_ordered_new_to_brand_percentage_14d,
    _New_Columns.campaign_id,
    _New_Columns.campaign_name,
    _New_Columns.clicks,
    _New_Columns.cost,
    _New_Columns.date,
    _New_Columns.dpv_14d,
    _New_Columns.external_id,
    _New_Columns.external_name,
    _New_Columns.impressions,
    _New_Columns.portfolio_id,
    _New_Columns.table,
    _New_Columns.units_sold_14d,
    _New_Columns.dsp_total_units_sold,
    _New_Columns.dsp_total_purchases,
    _New_Columns.dsp_total_product_sales,
    _New_Columns.ad_type,
    _New_Columns.metric_deleted_at,
    _New_Columns.target_id,
    _New_Columns.advertising_targets_text,
    _New_Columns.advertising_campaigns_category,
    _New_Columns.advertising_campaigns_name,
    _New_Columns.promotion,
    _New_Columns.advertising_campaigns_targeting_type,
    _New_Columns.advertising_campaigns_type,
    _New_Columns.advertising_portfolios_name,
    _New_Columns.budget_currency_code,
    _New_Columns.brand_id,
    _New_Columns.use_dsp_budget,
    _New_Columns.profile_id,
    _New_Columns.marketplace_id,
    _New_Columns.master_portfolio_id,
    _New_Columns.master_portfolio_name,
    _New_Columns.master_portfolio_brand_id,
    _New_Columns.master_portfolio_use_dsp_budget,
    _New_Columns.portfolio_client_id,
    _New_Columns.portfoliobrandname,
    _New_Columns.clients_name,
    _New_Columns.year,
    _New_Columns.month,
    _New_Columns.matchtype,
    _New_Columns.category,
    _New_Columns.adtype,
    _New_Columns.cpc,
    _New_Columns.ctr,
    _New_Columns.revenue,
    _New_Columns.orders,
    _New_Columns.units,
    _New_Columns.targetingtype,
    _New_Columns.category_v2,
    _New_Columns.targetingmethod,
    _New_Columns.targettingtype,
    _New_Columns.targetting_level,
    _New_Columns.roas,
    _New_Columns.acos,
    _New_Columns.costperconv,
    _New_Columns.convrate,
    _New_Columns.ntbordersperc,
    _New_Columns.ntbunitsperc,
    _New_Columns.ntbsalesperc,
    _New_Columns.opc_amount,
    _New_Columns.opc,
    _New_Columns.adtype_old,
    _New_Columns.competitortactic,
    _New_Columns.campaignname,
    _New_Columns.category_-_new,
    _New_Columns.campaign_label_-_tactic,
    _New_Columns.ad_type_-_new,
    _New_Columns.short_name,
    _New_Columns.targeting_type_-_new,
    _New_Columns.strategy_-_new,
    _New_Columns.targeting_level_-_new,
    _New_Columns.match_type_-_new,
    _New_Columns.advertising_tactic
  FROM _Group_By
  RIGHT JOIN _New_Columns
    ON _Group_By.date = _New_Columns.date
    AND _Group_By.campaign_id = _New_Columns.campaign_id
), _Select_Columns AS (
  SELECT
    id AS id,
    date AS date,
    client_id AS customerid,
    clients_name AS customername,
    brand_id AS brandid,
    brands_name AS brandname,
    category,
    adtype,
    competitortactic,
    portfolio_id AS portfolioid,
    advertising_portfolios_name AS portfolioname,
    campaign_id AS campaignid,
    campaignname,
    targetingtype,
    ad_group_id AS adgroupid,
    ad_group_name AS adgroupname,
    external_id AS keywordid,
    external_name AS keywordtext,
    matchtype,
    impressions AS impressions,
    clicks AS clicks,
    cost AS cost,
    cpc,
    ctr,
    revenue,
    roas,
    acos,
    costperconv,
    convrate,
    orders,
    units,
    attributed_orders_new_to_brand_14d AS ntborders,
    ntbordersperc,
    attributed_units_ordered_new_to_brand_14d AS ntbunits,
    ntbunitsperc,
    attributed_sales_new_to_brand_14d AS ntbsales,
    ntbsalesperc,
    opc,
    attributed_detail_page_views_clicks_14d AS dpv,
    budget_currency_code AS currencycode,
    dsp_total_product_sales AS dsp_total_product_sales,
    dsp_total_purchases AS dsp_total_purchases,
    dsp_total_units_sold AS dsp_total_units_sold,
    target_id,
    portfolio_client_id,
    top_of_search_impression_share,
    targetting_level,
    category_v2,
    targetingmethod,
    targettingtype,
    category_-_new,
    ad_type_-_new,
    targeting_type_-_new,
    short_name,
    campaign_label_-_tactic,
    strategy_-_new,
    targeting_level_-_new,
    match_type_-_new,
    promotion,
    advertising_keywords_text AS keyword_name,
    advertising_tactic,
    table
  FROM _Joining_with_campaign_performance
), _Append_Rows AS (
  SELECT
    id,
    date,
    customerid,
    customername,
    brandid,
    category,
    adtype,
    competitortactic,
    portfolioid,
    portfolioname,
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
    dpv,
    currencycode,
    dsp_total_product_sales,
    dsp_total_purchases,
    dsp_total_units_sold,
    target_id,
    portfolio_client_id,
    top_of_search_impression_share,
    targetting_level,
    category_v2,
    targetingmethod,
    targettingtype,
    category_-_new,
    ad_type_-_new,
    targeting_type_-_new,
    short_name,
    campaign_label_-_tactic,
    strategy_-_new,
    targeting_level_-_new,
    match_type_-_new,
    promotion,
    advertising_tactic,
    table
  FROM _Select_Columns
  UNION ALL
  SELECT
    id,
    date,
    customerid,
    customername,
    brandid,
    CAST(category AS DECIMAL(38, 0)) AS category,
    CAST(NULL AS DECIMAL(38, 0)) AS adtype,
    CAST(NULL AS DECIMAL(38, 0)) AS competitortactic,
    portfolioid,
    portfolioname,
    campaignid,
    campaignname,
    CAST(NULL AS DECIMAL(38, 0)) AS targetingtype,
    adgroupid,
    adgroupname,
    keywordid,
    keywordtext,
    CAST(NULL AS DECIMAL(38, 0)) AS matchtype,
    impressions,
    clicks,
    cost,
    CAST(NULL AS DECIMAL(38, 0)) AS cpc,
    CAST(NULL AS DECIMAL(38, 0)) AS ctr,
    CAST(revenue AS VARCHAR) AS revenue,
    CAST(NULL AS DECIMAL(38, 0)) AS roas,
    CAST(NULL AS DECIMAL(38, 0)) AS acos,
    CAST(NULL AS DECIMAL(38, 0)) AS costperconv,
    CAST(NULL AS DECIMAL(38, 0)) AS convrate,
    CAST(orders AS VARCHAR) AS orders,
    CAST(NULL AS VARCHAR) AS units,
    CAST(NULL AS BIGINT) AS ntborders,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbordersperc,
    CAST(NULL AS BIGINT) AS ntbunits,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbunitsperc,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbsales,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbsalesperc,
    CAST(NULL AS VARCHAR) AS opc,
    CAST(NULL AS BIGINT) AS dpv,
    CAST(NULL AS VARCHAR) AS currencycode,
    CAST(NULL AS DECIMAL(38, 0)) AS dsp_total_product_sales,
    CAST(NULL AS DECIMAL(38, 0)) AS dsp_total_purchases,
    CAST(NULL AS DECIMAL(38, 0)) AS dsp_total_units_sold,
    CAST(NULL AS VARCHAR) AS target_id,
    CAST(NULL AS BIGINT) AS portfolio_client_id,
    CAST(NULL AS DECIMAL(38, 0)) AS top_of_search_impression_share,
    CAST(NULL AS DECIMAL(38, 0)) AS targetting_level,
    CAST(NULL AS VARCHAR) AS category_v2,
    CAST(NULL AS VARCHAR) AS targetingmethod,
    CAST(NULL AS DECIMAL(38, 0)) AS targettingtype,
    CAST(NULL AS DECIMAL(38, 0)) AS category_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS ad_type_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS targeting_type_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS short_name,
    CAST(NULL AS DECIMAL(38, 0)) AS campaign_label_-_tactic,
    CAST(NULL AS DECIMAL(38, 0)) AS strategy_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS targeting_level_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS match_type_-_new,
    CAST(NULL AS VARCHAR) AS promotion,
    CAST(NULL AS DECIMAL(38, 0)) AS advertising_tactic,
    CAST(NULL AS VARCHAR) AS table
  FROM _Walmart_Formulas
  UNION ALL
  SELECT
    CAST(NULL AS BIGINT) AS id,
    date,
    CAST(NULL AS BIGINT) AS customerid,
    CAST(NULL AS VARCHAR) AS customername,
    CAST(brandid AS BIGINT) AS brandid,
    category,
    CAST(NULL AS DECIMAL(38, 0)) AS adtype,
    CAST(NULL AS DECIMAL(38, 0)) AS competitortactic,
    portfolioid,
    portfolioname,
    CAST(campaignid AS VARCHAR) AS campaignid,
    campaignname,
    CAST(NULL AS DECIMAL(38, 0)) AS targetingtype,
    CAST(NULL AS VARCHAR) AS adgroupid,
    CAST(NULL AS VARCHAR) AS adgroupname,
    CAST(NULL AS VARCHAR) AS keywordid,
    CAST(NULL AS VARCHAR) AS keywordtext,
    CAST(NULL AS DECIMAL(38, 0)) AS matchtype,
    CAST(impressions AS BIGINT) AS impressions,
    CAST(clicks AS BIGINT) AS clicks,
    CAST(cost AS DOUBLE) AS cost,
    CAST(NULL AS DECIMAL(38, 0)) AS cpc,
    CAST(NULL AS DECIMAL(38, 0)) AS ctr,
    CAST(NULL AS VARCHAR) AS revenue,
    CAST(NULL AS DECIMAL(38, 0)) AS roas,
    CAST(NULL AS DECIMAL(38, 0)) AS acos,
    CAST(NULL AS DECIMAL(38, 0)) AS costperconv,
    CAST(NULL AS DECIMAL(38, 0)) AS convrate,
    CAST(orders AS VARCHAR) AS orders,
    CAST(NULL AS VARCHAR) AS units,
    CAST(NULL AS BIGINT) AS ntborders,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbordersperc,
    CAST(NULL AS BIGINT) AS ntbunits,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbunitsperc,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbsales,
    CAST(NULL AS DECIMAL(38, 0)) AS ntbsalesperc,
    CAST(NULL AS VARCHAR) AS opc,
    CAST(NULL AS BIGINT) AS dpv,
    CAST(NULL AS VARCHAR) AS currencycode,
    CAST(NULL AS DECIMAL(38, 0)) AS dsp_total_product_sales,
    CAST(NULL AS DECIMAL(38, 0)) AS dsp_total_purchases,
    CAST(NULL AS DECIMAL(38, 0)) AS dsp_total_units_sold,
    CAST(NULL AS VARCHAR) AS target_id,
    CAST(NULL AS BIGINT) AS portfolio_client_id,
    CAST(NULL AS DECIMAL(38, 0)) AS top_of_search_impression_share,
    CAST(NULL AS DECIMAL(38, 0)) AS targetting_level,
    CAST(NULL AS VARCHAR) AS category_v2,
    CAST(NULL AS VARCHAR) AS targetingmethod,
    CAST(NULL AS DECIMAL(38, 0)) AS targettingtype,
    CAST(NULL AS DECIMAL(38, 0)) AS category_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS ad_type_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS targeting_type_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS short_name,
    CAST(NULL AS DECIMAL(38, 0)) AS campaign_label_-_tactic,
    CAST(NULL AS DECIMAL(38, 0)) AS strategy_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS targeting_level_-_new,
    CAST(NULL AS DECIMAL(38, 0)) AS match_type_-_new,
    CAST(NULL AS VARCHAR) AS promotion,
    CAST(NULL AS DECIMAL(38, 0)) AS advertising_tactic,
    CAST(NULL AS VARCHAR) AS table
  FROM _Join_Data_7
), _Advertising_Metrics_POC AS (
  SELECT
    *
  FROM _Append_Rows
)
SELECT
  *
FROM _Advertising_Metrics_POC