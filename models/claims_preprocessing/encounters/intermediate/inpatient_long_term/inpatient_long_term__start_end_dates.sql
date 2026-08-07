{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

-- group by patient_data_source_id too, so rows from different patients aren't merged
select encounter_id
, patient_data_source_id
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('inpatient_long_term__generate_encounter_id') }}
group by encounter_id
, patient_data_source_id
