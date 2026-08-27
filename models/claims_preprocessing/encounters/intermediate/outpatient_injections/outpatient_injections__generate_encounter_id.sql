{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select patient_data_source_id
, data_source
, start_date
, {{ the_tuva_project.encounter_id_hash(["'outpatient injections'", 'patient_data_source_id', 'start_date']) }} as old_encounter_id
from {{ ref('outpatient_injections__anchor_events') }}
