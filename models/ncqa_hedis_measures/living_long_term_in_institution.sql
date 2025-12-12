

select
  person_id as person_id,
  enrollment_start_date as start_date,
  enrollment_end_date as end_date
from {{ ref('eligibility') }}
where living_long_term_in_institution = 1
