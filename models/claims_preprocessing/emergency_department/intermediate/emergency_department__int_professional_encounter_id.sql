{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


with emergency_department_professional_claim_ids as (
select distinct claim_id
from {{ ref('service_category__service_category_grouper') }} 
where claim_type = 'professional'
  and service_category_2 = 'Emergency Department'
),

emergency_department_professional_claim_lines as (
select
  mc.claim_id,
  mc.patient_id,
  mc.claim_start_date as start_date,
  mc.claim_end_date as end_date	   
from {{ ref('medical_claim') }} mc
inner join emergency_department_professional_claim_ids prof
  on mc.claim_id = prof.claim_id
),


emergency_department_professional_claim_dates as (
select
  claim_id,
  patient_id,
  min(start_date) as start_date,
  max(end_date) as end_date
from emergency_department_professional_claim_lines
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
from emergency_department_professional_claim_dates aa
left join {{ ref('emergency_department__int_encounter_start_and_end_dates') }} bb
  on aa.patient_id = bb.patient_id
  and (coalesce(aa.start_date, aa.end_date) between coalesce(bb.encounter_start_date, bb.determined_encounter_start_date) and coalesce(bb.encounter_end_date, bb.determined_encounter_end_date))
  and (coalesce(aa.end_date, aa.start_date) between coalesce(bb.encounter_start_date, bb.determined_encounter_start_date) and coalesce(bb.encounter_end_date, bb.determined_encounter_end_date))
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
having count(distinct encounter_id) > 1
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


all_emergency_department_professional_claims as (
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



select *, '{{ var('tuva_last_run')}}' as tuva_last_run
from all_emergency_department_professional_claims

