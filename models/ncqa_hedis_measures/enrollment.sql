
select
  person_id as person_id,
  enrollment_start_date as enrollment_start_date,
  enrollment_end_date as enrollment_end_date,
  payer as payer,
  payer_type as payer_type,
  0 as institutional_special_needs_plan
from {{ ref('core__eligibility') }}
