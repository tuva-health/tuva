{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select 
    person_id
  , year_month
  , payer 
  , count(distinct person_id, payer) as member_month
from 
    {{ ref('core__member_months') }} 
group by 
    person_id
  , year_month
  , payer 
