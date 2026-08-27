{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

/*
  Each source-specific encounter must number its own claims starting at one.
  Reused anchor claim IDs in different sources must not share a window.
*/

select
    'acute_inpatient' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('acute_inpatient__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'emergency_department' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('emergency_department__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'inpatient_hospice' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('inpatient_hospice__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'inpatient_long_term' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('inpatient_long_term__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'inpatient_psych' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('inpatient_psych__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'inpatient_rehab' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('inpatient_rehab__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'inpatient_snf' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('inpatient_snf__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1

union all

select
    'inpatient_substance_use' as encounter_type
  , patient_data_source_id
  , encounter_id
  , min(encounter_claim_number) as first_claim_number
from {{ ref('inpatient_substance_use__generate_encounter_id') }}
group by patient_data_source_id, encounter_id
having min(encounter_claim_number) <> 1
