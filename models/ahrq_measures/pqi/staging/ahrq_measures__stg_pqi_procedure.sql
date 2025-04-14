
select
    normalized_code_type
  , normalized_code
  , encounter_id
  , data_source
from
    {{ ref('core__procedure') }}
