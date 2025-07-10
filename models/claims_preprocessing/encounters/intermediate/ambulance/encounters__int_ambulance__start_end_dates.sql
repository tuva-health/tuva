select distinct
    encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
from {{ ref('encounters__int_ambulance__generate_encounter_id') }}