{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select patient_data_source_id
, start_date
, hcpcs_code
, {{ dbt_utils.generate_surrogate_key(['patient_data_source_id', 'start_date', 'hcpcs_code']) }} as old_encounter_id
from {{ ref('int_encounter__outpatient_radiology_anchor_event') }}
