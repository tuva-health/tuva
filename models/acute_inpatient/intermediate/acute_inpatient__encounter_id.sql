{{ config(
     enabled = var('acute_inpatient_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model assigns an encounter_id to each
-- institutional or professional acute inpatient claim
-- that is eligible to be part of an encounter.
-- Professional acute inpatient claims that are
-- orphan claims (don't overlap with an institutional
-- acute inpatient claim) or that have
-- encounter_count > 1 (overlap with more than one different
-- acute inpatient encounter) are not included here.
-- It returns a table with these 3 columns:
--      patient_id
--      claim_id
--      encounter_id
-- *************************************************




select
  patient_id,
  claim_id,
  encounter_id,
  '{{ var('last_update')}}' as last_update
from {{ ref('acute_inpatient__institutional_encounter_id') }}

union distinct

select
  patient_id,
  claim_id,
  encounter_id,
  '{{ var('last_update')}}' as last_update
from {{ ref('acute_inpatient__professional_encounter_id') }}
where (orphan_claim_flag = 0) and (encounter_count = 1)
