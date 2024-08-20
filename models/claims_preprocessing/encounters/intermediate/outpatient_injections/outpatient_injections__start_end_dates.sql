select old_encounter_id 
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('outpatient_injections__generate_encounter_id') }}
group by old_encounter_id
