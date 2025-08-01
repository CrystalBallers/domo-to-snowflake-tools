with
    source as (select * from {{ source("TEMP_ARGO_RAW", "DUMMY_PORTFOLIOS") }})

select
    "portfolioName" as portfolioname

from source