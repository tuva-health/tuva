{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select patient_data_source_id
, start_date
, dense_rank() over (
order by patient_data_source_id, start_date) as old_encounter_id
from {{ ref('int_encounter__outpatient_injections_anchor_event') }}
