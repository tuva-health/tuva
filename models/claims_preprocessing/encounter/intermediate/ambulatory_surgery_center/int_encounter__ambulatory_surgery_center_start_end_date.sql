{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select
    patient_data_source_id
  , old_encounter_id
  , min(start_date) as encounter_start_date
  , max(end_date) as encounter_end_date
from {{ ref('int_encounter__ambulatory_surgery_center_generate_id') }}
group by
    patient_data_source_id
  , old_encounter_id
