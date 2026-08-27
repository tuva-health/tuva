{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select patient_data_source_id
, data_source
, start_date
, hcpcs_code
, {{ the_tuva_project.encounter_id_hash(["'outpatient radiology'", 'patient_data_source_id', 'start_date', 'hcpcs_code']) }} as old_encounter_id
from {{ ref('outpatient_radiology__anchor_events') }}
