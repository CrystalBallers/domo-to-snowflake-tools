-- ERROR: Could not retrieve columns for table DW_REPORTS.raw_domo.AMAZON_INBOUND_SHIPMENTS using role: DBT_ROLE
-- 
-- This file could not be generated because the table schema could not be retrieved.
-- 
-- Possible causes:
-- 1. Table DW_REPORTS.raw_domo.AMAZON_INBOUND_SHIPMENTS does not exist
-- 2. Insufficient permissions to access the table with role: DBT_ROLE
-- 3. Snowflake connection failed
-- 4. Warehouse not available or not specified
-- 5. Role DBT_ROLE does not have access to the table
-- 
-- Please verify the table exists and you have proper permissions with role: DBT_ROLE, then regenerate this file.

/*
TABLE: DW_REPORTS.raw_domo.AMAZON_INBOUND_SHIPMENTS
DATASET_ID: 79436288-d42c-40b8-9c06-7c1e1681d143
ROLE_USED: DBT_ROLE
GENERATED_AT: 2025-07-31 23:02:29
STATUS: FAILED
*/

-- Uncomment and modify the following template when the table is available:
/*
with
    source as (select * from {{ source("raw_domo", "AMAZON_INBOUND_SHIPMENTS") }})

select
    -- Add your columns here when table schema is available
    *

from source
*/
