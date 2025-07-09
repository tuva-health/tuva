select distinct
    encounter_id
    , encounter_start_date
    , encounter_end_date
from {{ ref('encounters__int_acute_inpatient__generate_encounter_id') }}
