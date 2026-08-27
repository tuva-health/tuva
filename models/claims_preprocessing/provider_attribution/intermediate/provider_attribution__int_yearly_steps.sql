{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false) and the_tuva_project.tuva_boolean_var('claims_enabled', false))
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
