{{ config(
     enabled = var('provider_attribution_enabled', var('claims_enabled', var('tuva_marts_enabled', True))) | as_bool
   )
}}

select
    npi
  , primary_taxonomy_code
  , primary_specialty_description
  , entity_type_description
from {{ ref('terminology__provider') }}
