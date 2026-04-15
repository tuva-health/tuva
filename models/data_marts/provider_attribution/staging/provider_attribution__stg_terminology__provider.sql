{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

select
    npi
  , primary_taxonomy_code
  , primary_specialty_description
  , entity_type_description
from {{ ref('provider_data__provider') }}
