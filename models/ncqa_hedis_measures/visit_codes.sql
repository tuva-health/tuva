
select
  person_id as person_id,
  source_code_type as code_system,
  source_code as code,
  recorded_date as start_date,
  recorded_date as end_date,
  null as modifier_1,
  null as modifier_2,
  null as modifier_3,
  null as modifier_4,
  null as modifier_5,
  null as from_lab_claim,
  claim_id as claim_id,
  null as admission_date,
  null as discharge_date
from {{ ref('core__condition') }}
