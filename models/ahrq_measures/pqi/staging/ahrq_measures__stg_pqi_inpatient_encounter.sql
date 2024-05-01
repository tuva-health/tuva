select 
    *
  , to_char(encounter_start_date, 'yyyy') as year_number
from 
    {{ ref('core__encounter') }}
where 
    encounter_type = 'acute inpatient'
