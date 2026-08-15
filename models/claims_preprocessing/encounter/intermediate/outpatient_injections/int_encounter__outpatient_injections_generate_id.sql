{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select patient_data_source_id
, start_date
, {{ dbt_utils.generate_surrogate_key(['patient_data_source_id', 'start_date']) }} as old_encounter_id
from {{ ref('int_encounter__outpatient_injections_anchor_event') }}
