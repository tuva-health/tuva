{{ config(
     enabled = var('provider_attribution_enabled', var('claims_enabled', var('tuva_marts_enabled', True))) | as_bool
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
