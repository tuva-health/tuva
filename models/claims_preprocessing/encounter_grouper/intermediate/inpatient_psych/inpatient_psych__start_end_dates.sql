{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select encounter_id
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('inpatient_psych__generate_encounter_id') }}
group by encounter_id
