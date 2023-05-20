{{ config(
     enabled = var('encounter_grouper_enabled',var('tuva_marts_enabled',True))
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
  encounter_id
from {{ ref('encounter_grouper__acute_inpatient_institutional_encounter_id') }}

union distinct

select
  patient_id,
  claim_id,
  encounter_id
from {{ ref('encounter_grouper__acute_inpatient_professional_encounter_id') }}
where (orphan_claim_flag = 0) and (encounter_count = 1)
