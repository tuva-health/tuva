{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- *************************************************
-- This dbt model returns relevant encounter-level
-- data for every professional or institutional
-- acute inpatient claim that is part of an encounter
-- (note that all institutional acute inpatienet claims
--  are part of an acute inpatient encounter, but only
--  professional acute inpatient claims that are not
--  orphan claims and that belong to one and only
--  one encounter, i.e. have encounter_count = 1,
--  are part of an acute inpatient encounter).
-- It returns a table with these columns:
--      patient_id
--      claim_id
--      start_date (date used for merging claims into encounters)
--      end_date (date used for merging claims into encounters)
--      encounter_id
--      encounter_start_date,
--      encounter_end_date,
--      encounter_admit_source_code,
--      encounter_admit_type_code,
--      encounter_discharge_disposition_code
-- *************************************************




with useful_fields_at_claim_id_level as (
select
  aa.patient_id,
  aa.claim_id,
  aa.admit_type_code,
  aa.admit_source_code,
  aa.discharge_disposition_code,
  aa.start_date,
  aa.end_date,

  bb.encounter_id,

  cc.encounter_start_date,
  cc.encounter_end_date

from {{ ref('acute_inpatient__institutional_claims') }} aa

left join
{{ ref('acute_inpatient__encounter_id') }} bb
on aa.claim_id = bb.claim_id
and aa.patient_id = bb.patient_id

left join
{{ ref('acute_inpatient__encounter_start_and_end_dates') }} cc
on bb.encounter_id = cc.encounter_id
and bb.patient_id = cc.patient_id
),



admit_codes as (
select
  encounter_id,
  max(admit_source_code) as encounter_admit_source_code,
  max(admit_type_code) as encounter_admit_type_code
from useful_fields_at_claim_id_level
where start_date = encounter_start_date
group by encounter_id
),


discharge_code as (
select
  encounter_id,
  max(discharge_disposition_code) as encounter_discharge_disposition_code
from useful_fields_at_claim_id_level
where end_date = encounter_end_date
group by encounter_id
),


all_useful_fields_at_claim_id_level as (
select
  aa.patient_id,
  aa.claim_id,
  aa.start_date,
  aa.end_date,
  aa.encounter_id,
  aa.encounter_start_date,
  aa.encounter_end_date,

  bb.encounter_admit_source_code,
  bb.encounter_admit_type_code,
  
  cc.encounter_discharge_disposition_code

from useful_fields_at_claim_id_level aa 
     left join admit_codes bb on aa.encounter_id = bb.encounter_id
     left join discharge_code cc on aa.encounter_id = cc.encounter_id
)



select 
 *, '{{ var('tuva_last_run')}}' as tuva_last_run
from all_useful_fields_at_claim_id_level 
