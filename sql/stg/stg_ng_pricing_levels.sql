-- ERROR: Could not retrieve columns for table DW_REPORTS.raw_domo.NG_PRICING_LEVELS using role: DBT_ROLE
-- 
-- This file could not be generated because the table schema could not be retrieved.
-- 
-- Possible causes:
-- 1. Table DW_REPORTS.raw_domo.NG_PRICING_LEVELS does not exist
-- 2. Insufficient permissions to access the table with role: DBT_ROLE
-- 3. Snowflake connection failed
-- 4. Warehouse not available or not specified
-- 5. Role DBT_ROLE does not have access to the table
-- 
-- Please verify the table exists and you have proper permissions with role: DBT_ROLE, then regenerate this file.

/*
TABLE: DW_REPORTS.raw_domo.NG_PRICING_LEVELS
DATASET_ID: 9dfb93e9-5875-4576-9dc0-9fbfd987dc4a
ROLE_USED: DBT_ROLE
GENERATED_AT: 2025-07-31 23:02:37
STATUS: FAILED
*/

-- Uncomment and modify the following template when the table is available:
/*
with
    source as (select * from {{ source("raw_domo", "NG_PRICING_LEVELS") }})

select
    -- Add your columns here when table schema is available
    *

from source
*/
