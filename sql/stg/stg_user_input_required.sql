-- ERROR: Could not retrieve columns for table DW_REPORTS.raw_domo.USER_INPUT_REQUIRED using role: DBT_ROLE
-- 
-- This file could not be generated because the table schema could not be retrieved.
-- 
-- Possible causes:
-- 1. Table DW_REPORTS.raw_domo.USER_INPUT_REQUIRED does not exist
-- 2. Insufficient permissions to access the table with role: DBT_ROLE
-- 3. Snowflake connection failed
-- 4. Warehouse not available or not specified
-- 5. Role DBT_ROLE does not have access to the table
-- 
-- Please verify the table exists and you have proper permissions with role: DBT_ROLE, then regenerate this file.

/*
TABLE: DW_REPORTS.raw_domo.USER_INPUT_REQUIRED
DATASET_ID: 22309d4a-185c-4534-af8f-7c479bcd2e13
ROLE_USED: DBT_ROLE
GENERATED_AT: 2025-07-31 23:02:45
STATUS: FAILED
*/

-- Uncomment and modify the following template when the table is available:
/*
with
    source as (select * from {{ source("raw_domo", "USER_INPUT_REQUIRED") }})

select
    -- Add your columns here when table schema is available
    *

from source
*/
