{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

with cte as (
  select stg.claim_id
  , stg.claim_line_number
  , stg.service_category_1
  , stg.service_category_2
  , stg.service_category_3
  , stg.claim_type
  , stg.claim_start_date
  , stg.claim_end_date
  , stg.start_date
  , stg.end_date
  , stg.patient_data_source_id
  , stg.data_source
  from {{ ref('encounters__stg_medical_claim') }} as stg
  left outer join {{ ref('encounters__combined_claim_line_crosswalk') }} as enc on stg.claim_id = enc.claim_id
  and
  stg.claim_line_number = enc.claim_line_number
  and
  stg.data_source = enc.data_source
  where enc.claim_id is null -- missing from encounter mapping table
)

select
  claim_id
, claim_line_number
, data_source
, {{ the_tuva_project.encounter_id_hash(["'orphaned claim'", 'patient_data_source_id', 'claim_id']) }} as encounter_id
, 'orphaned claim' as encounter_type
, 'other' as encounter_group
from cte
