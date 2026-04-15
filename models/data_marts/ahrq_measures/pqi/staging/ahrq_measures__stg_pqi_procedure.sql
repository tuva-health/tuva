{{ config(
    enabled = var('claims_enabled', False) | as_bool
) }}

select
    normalized_code_type
  , normalized_code
  , encounter_id
  , data_source
from
    {{ ref('core__procedure') }}
