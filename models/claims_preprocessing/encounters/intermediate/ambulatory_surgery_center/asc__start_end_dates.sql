{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select
    patient_data_source_id
  , old_encounter_id
  , min(start_date) as encounter_start_date
  , max(end_date) as encounter_end_date
from {{ ref('asc__generate_encounter_id') }}
group by
    patient_data_source_id
  , old_encounter_id
