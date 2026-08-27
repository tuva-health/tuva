{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select encounter_id
, min(anchor_claim_id) as anchor_claim_id
, min(start_date) as encounter_start_date
, max(end_date) as encounter_end_date
from {{ ref('inpatient_snf__generate_encounter_id') }}
group by encounter_id
