with calendar as (

    select 
          full_date 
        , year 
        , month
        , day
        , month_name
        , day_of_week_number
        , day_of_week_name
        , week_of_year
        , day_of_year
        , year_month
        , first_day_of_month
        , last_day_of_month
        , year_month_int
    from {{ ref('reference_data__calendar') }}

)

select * 
from calendar 