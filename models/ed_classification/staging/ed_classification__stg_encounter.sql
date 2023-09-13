select
    encounter_id
    , encounter_type
from {{ ref('core__encounter') }}