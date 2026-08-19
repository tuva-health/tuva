{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

select
    person_id
  , data_source
  , performance_year
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , step_description
  , allowed_amount
  , visits
from {{ ref('provider_attribution__provider_ranking') }}
where scope = 'yearly'
