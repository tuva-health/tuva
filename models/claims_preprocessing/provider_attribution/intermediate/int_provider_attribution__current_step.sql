{{ config(
     enabled = ((var('provider_attribution_enabled', False) | string | lower) == 'true')
           and ((var('claims_enabled', False) | string | lower) == 'true')
   )
}}

select
    person_id
  , data_source
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , step_description
  , allowed_amount
  , visits
from {{ ref('provider_attribution__provider_ranking') }}
where scope = 'current'
