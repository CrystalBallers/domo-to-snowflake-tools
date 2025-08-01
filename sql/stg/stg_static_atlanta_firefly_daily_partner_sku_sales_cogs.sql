with
    source as (select * from {{ source("TEMP_ARGO_RAW", "STATIC_ATLANTA_FIREFLY_DAILY_PARTNER_SKU_SALES_COGS") }})

select
    "ASIN" as asin,
    "Calendar Date TY" as calendar_date_ty,
    "Ext Ordered FBA COGS" as ext_ordered_fba_cogs,
    "Ext Ordered FBA Commission" as ext_ordered_fba_commission,
    "Ext Ordered FBA Fulfillment" as ext_ordered_fba_fulfillment,
    "Ext Ordered FBA Sales" as ext_ordered_fba_sales,
    "Ext Ordered FBA Units" as ext_ordered_fba_units,
    "Ext Ordered NonFBA COGS" as ext_ordered_nonfba_cogs,
    "Ext Ordered NonFBA Commission" as ext_ordered_nonfba_commission,
    "Ext Ordered NonFBA Fulfillment" as ext_ordered_nonfba_fulfillment,
    "Ext Ordered NonFBA Sales" as ext_ordered_nonfba_sales,
    "Ext Ordered NonFBA Units" as ext_ordered_nonfba_units,
    "Ext Shipped FBA COGS" as ext_shipped_fba_cogs,
    "Ext Shipped FBA Commission" as ext_shipped_fba_commission,
    "Ext Shipped FBA Fulfillment" as ext_shipped_fba_fulfillment,
    "Ext Shipped FBA Sales" as ext_shipped_fba_sales,
    "Ext Shipped FBA Units" as ext_shipped_fba_units,
    "Ext Shipped NonFBA COGS" as ext_shipped_nonfba_cogs,
    "Ext Shipped NonFBA Commission" as ext_shipped_nonfba_commission,
    "Ext Shipped NonFBA Fulfillment" as ext_shipped_nonfba_fulfillment,
    "Ext Shipped NonFBA Sales" as ext_shipped_nonfba_sales,
    "Ext Shipped NonFBA Units" as ext_shipped_nonfba_units,
    "Partner" as partner,
    "PartnerSKUCalendarKey" as partnerskucalendarkey,
    "SKU" as sku,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source