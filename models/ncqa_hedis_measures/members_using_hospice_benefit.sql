

select
  person_id as person_id,
  enrollment_start_date as start_date,
  enrollment_end_date as end_date
from {{ ref('eligibility') }}
where using_hospice_benefit = 1
