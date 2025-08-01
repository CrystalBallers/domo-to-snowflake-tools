with
    source as (select * from {{ source("TEMP_ARGO_RAW", "CALENDAR_DAYS_WEEKS_CONSTANTS") }})

select
    "Key" as key,
    "All Days" as all_days,
    "All Weeks" as all_weeks,
    "Yearweek" as yearweek,
    "WeekCheat" as weekcheat,
    "Variable_Set_ID" as variable_set_id,
    "Remove_MOC >___Months" as remove_moc____months,
    "Hours (9000~24*375)" as hours_900024375

from source