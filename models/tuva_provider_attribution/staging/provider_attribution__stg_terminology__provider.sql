{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select
    npi
  , primary_taxonomy_code
  , primary_specialty_description
  , entity_type_description
from {{ ref('terminology__provider') }}
