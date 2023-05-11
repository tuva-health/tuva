{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model assigns professional acute inpatient
-- claims to acute inpatient encounters.
-- Some acute inpatient professional claims do not fall
-- within an acute inpatient encounter. We call these
-- orphan claims, and they do not have an encounter_id.
-- Some acute inpatient professional claims fall within
-- more than one acute inpatient encounter. Since we do not
-- know what encounter these claims should be mapped to
-- we do not assign them an encounter_id. These claims
-- have encounter_count > 1.
-- This model returns a table with 5 fields:
--      patient_id
--      claim_id
--      encounter_id (is null for orphan claims or claims
--                    that fall within more than one encounter)
--      orphan_claim_flag (is always 0 or 1)
--      encounter_count (an integer that represents the number
--                       of acute inpatient encounters this
--                       professional claim could fall into:
--                       could be 0,1,2,3,...  but is never null
--                       for acute inpatient professional claims)
-- *************************************************




with acute_inpatient_professional_claim_ids as (
select distinct claim_id
from {{ ref('input_layer__medical_claim') }} 
where place_of_service_code = '21'
-- Do we include a requirement for claim_type = 'professional'
-- to avoid having institutional claims where a place of service
-- code was imputed?
),


acute_inpatient_professional_claim_lines as (
select
  mc.claim_id,
  mc.patient_id,
  coalesce(mc.claim_start_date,
           mc.claim_line_start_date,
	   mc.claim_end_date,
	   mc.claim_line_end_date) as start_date,
  coalesce(mc.claim_end_date,
           mc.claim_line_end_date,
	   mc.claim_start_date,
	   mc.claim_line_start_date) as end_date	   
from {{ ref('input_layer__medical_claim') }} mc
     inner join acute_inpatient_professional_claim_ids prof
     on mc.claim_id = prof.claim_id
),


acute_inpatient_professional_claim_dates as (
select
  claim_id,
  patient_id,
  min(start_date) as start_date,
  max(end_date) as end_date
from acute_inpatient_professional_claim_lines
group by claim_id, patient_id
),


roll_up_professional_claims_to_institutional_claims as (
select
  aa.patient_id,
  aa.claim_id,
  aa.start_date,
  aa.end_date,
  bb.encounter_id,
  case
    when bb.encounter_id is null then 1
    else 0
  end as orphan_claim_flag
  
from acute_inpatient_professional_claim_dates aa
left join
{{ ref('claims_preprocessing__acute_inpatient_encounter_start_and_end_dates') }} bb
on aa.patient_id = bb.patient_id
and (aa.start_date
     between bb.encounter_start_date and bb.encounter_end_date)
and (aa.end_date
     between bb.encounter_start_date and bb.encounter_end_date)
),


professional_claims_in_more_than_one_encounter as (
select
  patient_id,
  claim_id,
  min(start_date) as start_date,
  max(end_date) as end_date,
  count(distinct encounter_id) as encounter_count
from roll_up_professional_claims_to_institutional_claims
group by patient_id, claim_id
having encounter_count > 1
),


professional_claims_not_in_more_than_one_encounter as (
select
  aa.patient_id,
  aa.claim_id,
  aa.start_date,
  aa.end_date,
  aa.encounter_id,
  aa.orphan_claim_flag,
  case
    when (aa.orphan_claim_flag = 1) then 0
    else 1
  end as encounter_count
from roll_up_professional_claims_to_institutional_claims aa
left join professional_claims_in_more_than_one_encounter bb
on aa.claim_id = bb.claim_id
and aa.patient_id = bb.patient_id
where (bb.patient_id is null) and (bb.claim_id is null)
),


all_acute_inpatient_professional_claims as (
select
  patient_id,
  claim_id,
  start_date,
  end_date,
  encounter_id,
  orphan_claim_flag,
  encounter_count
from professional_claims_not_in_more_than_one_encounter

union all

select
  patient_id,
  claim_id,
  start_date,
  end_date,
  null as encounter_id,
  0 as orphan_claim_count,
  encounter_count
from professional_claims_in_more_than_one_encounter
)



select *
from all_acute_inpatient_professional_claims

