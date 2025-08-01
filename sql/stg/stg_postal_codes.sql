with
    source as (select * from {{ source("TEMP_ARGO_RAW", "POSTAL_CODES") }})

select
    "area_code" as area_code,
    "city" as city,
    "code" as code,
    "country" as country,
    "country_code" as country_code,
    "county" as county,
    "deleted_at" as deleted_at,
    "housing_units" as housing_units,
    "id" as id,
    "land_area" as land_area,
    "latitude" as latitude,
    "longitude" as longitude,
    "median_home_value" as median_home_value,
    "median_household_income" as median_household_income,
    "occupied_housing_units" as occupied_housing_units,
    "population" as population,
    "population_density" as population_density,
    "state" as state,
    "state_code" as state_code,
    "timezone" as timezone,
    "type" as type,
    "water_area" as water_area,
    "_BATCH_ID_" as _batch_id_,
    "_BATCH_LAST_RUN_" as _batch_last_run_

from source