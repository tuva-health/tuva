{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

-- Oasis fix: group by patient_data_source_id in addition to encounter_id so that if two
-- different patients' rows ever land on the same encounter_id, their date ranges are not
-- merged together.
select encounter_id
, patient_data_source_id
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('acute_inpatient__generate_encounter_id') }}
group by encounter_id
, patient_data_source_id
