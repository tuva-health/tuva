{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

select
    person_id
  , year_month
from {{ ref('core__member_months') }}
