{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

select
    full_date
    , year
    , year_month_int
    , first_day_of_month
from {{ ref('reference_data__calendar') }}
