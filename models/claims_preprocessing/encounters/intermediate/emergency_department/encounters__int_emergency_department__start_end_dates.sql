select distinct
    encounter_id
    , encounter_start_date
    , encounter_end_date
from {{ ref('encounters__int_emergency_department__generate_encounter_id') }}