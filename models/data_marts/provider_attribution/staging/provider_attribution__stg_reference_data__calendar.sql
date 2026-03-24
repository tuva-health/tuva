{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

select
    full_date
  , year
  , month
  , year_month
  , first_day_of_month
  , last_day_of_month
  , year_month_int
from {{ ref('reference_data__calendar') }}
