
select encounter_id
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('emergency_department__generate_encounter_id') }}
group by encounter_id
