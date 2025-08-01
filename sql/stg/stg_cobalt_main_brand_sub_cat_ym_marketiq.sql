-- ERROR: Could not retrieve columns for table DW_REPORTS.raw_domo.COBALT_MAIN_BRAND_SUB_CAT_YM_MARKETIQ using role: DBT_ROLE
-- 
-- This file could not be generated because the table schema could not be retrieved.
-- 
-- Possible causes:
-- 1. Table DW_REPORTS.raw_domo.COBALT_MAIN_BRAND_SUB_CAT_YM_MARKETIQ does not exist
-- 2. Insufficient permissions to access the table with role: DBT_ROLE
-- 3. Snowflake connection failed
-- 4. Warehouse not available or not specified
-- 5. Role DBT_ROLE does not have access to the table
-- 
-- Please verify the table exists and you have proper permissions with role: DBT_ROLE, then regenerate this file.

/*
TABLE: DW_REPORTS.raw_domo.COBALT_MAIN_BRAND_SUB_CAT_YM_MARKETIQ
DATASET_ID: 6be4db2f-db04-49a8-96fd-3cef4b42704b
ROLE_USED: DBT_ROLE
GENERATED_AT: 2025-07-31 23:02:32
STATUS: FAILED
*/

-- Uncomment and modify the following template when the table is available:
/*
with
    source as (select * from {{ source("raw_domo", "COBALT_MAIN_BRAND_SUB_CAT_YM_MARKETIQ") }})

select
    -- Add your columns here when table schema is available
    *

from source
*/
