select
    full_date
    , year
    , year_month_int
    , first_day_of_month
from {{ ref('reference_data__calendar') }}
