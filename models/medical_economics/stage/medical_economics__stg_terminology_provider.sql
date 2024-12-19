{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select 
      npi 
    , entity_type_description
    , primary_taxonomy_code
    , primary_specialty_description
    , provider_first_name
    , provider_last_name
    , provider_organization_name
    , parent_organization_name
    , 'hello_world' as specialty
--add specialty from crosswalk file created
from 
    {{ ref('terminology__provider') }} 
