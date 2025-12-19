{{ config(
     enabled = var('provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
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
