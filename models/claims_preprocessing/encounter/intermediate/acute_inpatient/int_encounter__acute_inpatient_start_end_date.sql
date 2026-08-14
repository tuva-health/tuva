{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select encounter_id
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('int_encounter__acute_inpatient_generate_id') }}
group by encounter_id
