{{ config(
     enabled = var('provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select
    person_id
  , year_month
from {{ ref('core__member_months') }}
