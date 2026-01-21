{{ config(
     enabled = (  var('hedis_measures_enabled', False) == True  and
                  (var('claims_enabled', False) == True or var('clinical_enabled', False) == True)
               ) | as_bool
   )
}}

with claims_with_pos_81 as (
select distinct 
  claim_id
, person_id
from {{ ref('core__medical_claim') }}
where place_of_service_code = 81
),

claims_admission_and_discharge_dates as (
select
  claim_id,
  person_id,
  max(admission_date) as admission_date,
  max(discharge_date) as discharge_date
from {{ ref('core__medical_claim') }}
group by claim_id, person_id
),

bill_type_codes as (
select
  claim_id,
  person_id,
  max(bill_type_code) as bill_type_code,
  max(claim_start_date) as claim_start_date,
  max(claim_end_date) as claim_end_date,
  max(admission_date) as admission_date,
  max(discharge_date) as discharge_date
from {{ ref('core__medical_claim') }}
where bill_type_code is not null
group by claim_id, person_id
),

bill_type as (
select
  aa.person_id as person_id,
  'bill_type_code' as code_system,
  aa.bill_type_code as code,
  coalesce(aa.claim_start_date,
           aa.claim_end_date,
	   aa.admission_date,
	   aa.discharge_date) as start_date,
  coalesce(aa.claim_start_date,
           aa.claim_end_date,
	   aa.admission_date,
	   aa.discharge_date) as end_date,
  null as modifier_1,
  null as modifier_2,
  null as modifier_3,
  null as modifier_4,
  null as modifier_5,
  case
    when bb.person_id is not null then 1
    else 0
  end as from_lab_claim,
  aa.claim_id as claim_id,
  aa.admission_date as admission_date,
  aa.discharge_date as discharge_date
from bill_type_codes aa
left join claims_with_pos_81 bb
  on aa.claim_id = bb.claim_id 
  and aa.person_id = bb.person_id
),

conditions as (
select
  aa.person_id as person_id,
  aa.normalized_code_type as code_system,
  aa.normalized_code as code,
  aa.recorded_date as start_date,
  aa.recorded_date as end_date,
  null as modifier_1,
  null as modifier_2,
  null as modifier_3,
  null as modifier_4,
  null as modifier_5,
  case
    when bb.person_id is not null then 1
    else 0
  end as from_lab_claim,
  aa.claim_id as claim_id,
  cc.admission_date as admission_date,
  cc.discharge_date as discharge_date
from {{ ref('core__condition') }} aa
left join claims_with_pos_81 bb
  on aa.person_id = bb.person_id 
  and aa.claim_id = bb.claim_id
left join claims_admission_and_discharge_dates cc
  on aa.person_id = cc.person_id 
  and aa.claim_id = cc.claim_id
where (aa.normalized_code is not null 
  and aa.normalized_code_type is not null)
),


procedures as (
select
  aa.person_id as person_id,
  aa.normalized_code_type as code_system,
  aa.normalized_code as code,
  aa.procedure_date as start_date,
  aa.procedure_date as end_date,
  modifier_1 as modifier_1,
  modifier_2 as modifier_2,
  modifier_3 as modifier_3,
  modifier_4 as modifier_4,
  modifier_5 as modifier_5,
  case
    when bb.person_id is not null then 1
    else 0
  end as from_lab_claim,
  aa.claim_id as claim_id,
  cc.admission_date as admission_date,
  cc.discharge_date as discharge_date
from {{ ref('core__procedure') }} aa
left join claims_with_pos_81 bb
  on aa.person_id = bb.person_id 
  and aa.claim_id = bb.claim_id
left join claims_admission_and_discharge_dates cc
  on aa.person_id = cc.person_id 
  and aa.claim_id = cc.claim_id
where (aa.normalized_code is not null 
  and aa.normalized_code_type is not null)
),

revenue_codes as (
select
  aa.person_id as person_id,
  'revenue_center_code' as code_system,
  revenue_center_code as code,
  coalesce(aa.claim_start_date,
           aa.claim_end_date,
	   aa.admission_date,
	   aa.discharge_date) as start_date,
  coalesce(aa.claim_start_date,
           aa.claim_end_date,
	   aa.admission_date,
	   aa.discharge_date) as end_date,
  null as modifier_1,
  null as modifier_2,
  null as modifier_3,
  null as modifier_4,
  null as modifier_5,
  case
    when bb.person_id is not null then 1
    else 0
  end as from_lab_claim,
  aa.claim_id as claim_id,
  aa.admission_date as admission_date,
  aa.discharge_date as discharge_date
from {{ ref('core__medical_claim') }} aa
left join claims_with_pos_81 bb
  on aa.person_id = bb.person_id 
  and aa.claim_id = bb.claim_id
where aa.revenue_center_code is not null
)

select distinct
  person_id,
  code_system,
  code,
  0 as principal_diagnosis,
  start_date,
  end_date,
  modifier_1,
  modifier_2,
  modifier_3,
  modifier_4,
  modifier_5,
  from_lab_claim,
  claim_id,
  admission_date,
  discharge_date,
  0 as from_prescribing_provider_or_clinical_pharmacist,
  0 as from_eye_care_provider
from (
select * from bill_type
union all
select * from conditions
union all
select * from procedures
union all
select * from revenue_codes
)
