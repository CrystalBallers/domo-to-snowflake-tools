/*
================================================================================
DOMO DATAFLOW TRANSLATION
================================================================================
Dataflow ID: 1113
Dataflow Name: Advertising Walmart Metrics POC DF
Target Dialect: MYSQL

TRANSLATION SUMMARY:
  Total Actions: 12
  Successful: 12
  Failed: 0
  Unique Action Types: 5
  Action Types: ExpressionEvaluator, LoadFromVault, MergeJoin, PublishToVault, SelectValues
  Pipelines: 4

Generated: 2025-07-02 10:24:24
================================================================================
*/
WITH _advertising_walmart_metrics AS (
  SELECT
    ad_group_id,
    ad_group_name,
    campaign_id,
    campaign_name,
    clicks,
    cost,
    created_at,
    date,
    id,
    impressions,
    keyword_id,
    keyword_text,
    orders,
    portfolio_id,
    portfolio_name,
    sales,
    updated_at,
    updated_date,
    batch_id,
    batch_last_run
  FROM advertising_walmart_metrics
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
), _Metrics_Portfolios AS (
  SELECT
    _advertising_portfolios.name,
    _advertising_portfolios.budget_amount,
    _advertising_portfolios.budget_currency_code,
    _advertising_portfolios.budget_policy,
    _advertising_portfolios.in_budget,
    _advertising_portfolios.state,
    _advertising_portfolios.deleted_at,
    _advertising_portfolios.opc,
    _advertising_portfolios.brand_id,
    _advertising_portfolios.use_dsp_budget,
    _advertising_portfolios.profile_id,
    _advertising_portfolios.marketplace_id,
    _advertising_portfolios.master_portfolio_id,
    _advertising_portfolios.master_portfolio_name,
    _advertising_portfolios.master_portfolio_brand_id,
    _advertising_portfolios.master_portfolio_use_dsp_budget,
    _advertising_portfolios.portfolio_client_id,
    _advertising_portfolios.portfoliobrandname,
    _advertising_walmart_metrics.ad_group_id,
    _advertising_walmart_metrics.ad_group_name,
    _advertising_walmart_metrics.campaign_id,
    _advertising_walmart_metrics.campaign_name,
    _advertising_walmart_metrics.clicks,
    _advertising_walmart_metrics.cost,
    _advertising_walmart_metrics.created_at,
    _advertising_walmart_metrics.date,
    _advertising_walmart_metrics.id,
    _advertising_walmart_metrics.impressions,
    _advertising_walmart_metrics.keyword_id,
    _advertising_walmart_metrics.keyword_text,
    _advertising_walmart_metrics.orders,
    _advertising_walmart_metrics.portfolio_id,
    _advertising_walmart_metrics.portfolio_name,
    _advertising_walmart_metrics.sales,
    _advertising_walmart_metrics.updated_at,
    _advertising_walmart_metrics.updated_date,
    _advertising_walmart_metrics.batch_id,
    _advertising_walmart_metrics.batch_last_run
  FROM _advertising_portfolios
  INNER JOIN _advertising_walmart_metrics
    ON _advertising_portfolios.portfolio_id = _advertising_walmart_metrics.portfolio_id
), _Brands_columns AS (
  SELECT
    name AS brand,
    id AS brand_id,
    client_id AS client_id
  FROM _Brands
), _Client_columns AS (
  SELECT
    name AS partner,
    id AS client_id
  FROM _clients
), _Add_Brand_Info AS (
  SELECT
    _Metrics_Portfolios.name,
    _Metrics_Portfolios.budget_amount,
    _Metrics_Portfolios.budget_currency_code,
    _Metrics_Portfolios.budget_policy,
    _Metrics_Portfolios.in_budget,
    _Metrics_Portfolios.state,
    _Metrics_Portfolios.deleted_at,
    _Metrics_Portfolios.opc,
    _Metrics_Portfolios.use_dsp_budget,
    _Metrics_Portfolios.profile_id,
    _Metrics_Portfolios.marketplace_id,
    _Metrics_Portfolios.master_portfolio_id,
    _Metrics_Portfolios.master_portfolio_name,
    _Metrics_Portfolios.master_portfolio_brand_id,
    _Metrics_Portfolios.master_portfolio_use_dsp_budget,
    _Metrics_Portfolios.portfolio_client_id,
    _Metrics_Portfolios.portfoliobrandname,
    _Metrics_Portfolios.ad_group_id,
    _Metrics_Portfolios.ad_group_name,
    _Metrics_Portfolios.campaign_id,
    _Metrics_Portfolios.campaign_name,
    _Metrics_Portfolios.clicks,
    _Metrics_Portfolios.cost,
    _Metrics_Portfolios.created_at,
    _Metrics_Portfolios.date,
    _Metrics_Portfolios.id,
    _Metrics_Portfolios.impressions,
    _Metrics_Portfolios.keyword_id,
    _Metrics_Portfolios.keyword_text,
    _Metrics_Portfolios.orders,
    _Metrics_Portfolios.portfolio_id,
    _Metrics_Portfolios.portfolio_name,
    _Metrics_Portfolios.sales,
    _Metrics_Portfolios.updated_at,
    _Metrics_Portfolios.updated_date,
    _Metrics_Portfolios.batch_id,
    _Metrics_Portfolios.batch_last_run,
    _Brands_columns.brand,
    _Brands_columns.brand_id,
    _Brands_columns.client_id
  FROM _Metrics_Portfolios
  LEFT JOIN _Brands_columns
    ON _Metrics_Portfolios.brand_id = _Brands_columns.brand_id
), _Join_Data AS (
  SELECT
    _Add_Brand_Info.name,
    _Add_Brand_Info.budget_amount,
    _Add_Brand_Info.budget_currency_code,
    _Add_Brand_Info.budget_policy,
    _Add_Brand_Info.in_budget,
    _Add_Brand_Info.state,
    _Add_Brand_Info.deleted_at,
    _Add_Brand_Info.opc,
    _Add_Brand_Info.use_dsp_budget,
    _Add_Brand_Info.profile_id,
    _Add_Brand_Info.marketplace_id,
    _Add_Brand_Info.master_portfolio_id,
    _Add_Brand_Info.master_portfolio_name,
    _Add_Brand_Info.master_portfolio_brand_id,
    _Add_Brand_Info.master_portfolio_use_dsp_budget,
    _Add_Brand_Info.portfolio_client_id,
    _Add_Brand_Info.portfoliobrandname,
    _Add_Brand_Info.ad_group_id,
    _Add_Brand_Info.ad_group_name,
    _Add_Brand_Info.campaign_id,
    _Add_Brand_Info.campaign_name,
    _Add_Brand_Info.clicks,
    _Add_Brand_Info.cost,
    _Add_Brand_Info.created_at,
    _Add_Brand_Info.date,
    _Add_Brand_Info.id,
    _Add_Brand_Info.impressions,
    _Add_Brand_Info.keyword_id,
    _Add_Brand_Info.keyword_text,
    _Add_Brand_Info.orders,
    _Add_Brand_Info.portfolio_id,
    _Add_Brand_Info.portfolio_name,
    _Add_Brand_Info.sales,
    _Add_Brand_Info.updated_at,
    _Add_Brand_Info.updated_date,
    _Add_Brand_Info.batch_id,
    _Add_Brand_Info.batch_last_run,
    _Add_Brand_Info.brand,
    _Add_Brand_Info.brand_id,
    _Client_columns.partner,
    _Client_columns.client_id
  FROM _Add_Brand_Info
  LEFT JOIN _Client_columns
    ON _Add_Brand_Info.client_id = _Client_columns.client_id
), _Select_Columns AS (
  SELECT
    id AS id,
    date AS date,
    client_id AS customerid,
    partner AS customername,
    brand_id AS brandid,
    brand AS brandname,
    portfolio_id AS portfolioid,
    portfolio_name AS portfolioname,
    campaign_id AS campaignid,
    campaign_name AS campaignname,
    ad_group_id AS adgroupid,
    ad_group_name AS adgroupname,
    impressions AS impressions,
    clicks AS clicks,
    cost AS cost,
    sales AS revenue,
    orders AS orders,
    keyword_id AS keywordid,
    keyword_text AS keywordtext
  FROM _Join_Data
), _Add_Formula AS (
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
    (
      brandid
    ) AS brand_id
  FROM _Select_Columns
), _Advertising_Walmart_Metrics_POC AS (
  SELECT
    *
  FROM _Add_Formula
)
SELECT
  *
FROM _Advertising_Walmart_Metrics_POC