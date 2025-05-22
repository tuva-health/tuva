
{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}



select
  data_source || '_' || claim_id || '_' || condition_rank as condition_id,
  person_id,
  member_id,
  null as patient_id,
  null as encounter_id,
  claim_id,
  recorded_date,
  null as onset_date,
  null as resolved_date,
  'active' as status,
  'discharge_diagnosis' as condition_type,
  source_code_type,
  source_code,
  null as source_description,
  normalized_code_type,
  normalized_code,
  normalized_description,
  'manual' as mapping_method,
  condition_rank,
  present_on_admit_code,
  present_on_admit_description,
  data_source
from {{ ref('core__claims_conditions_normalized') }}
