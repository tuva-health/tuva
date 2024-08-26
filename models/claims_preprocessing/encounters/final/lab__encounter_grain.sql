{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with encounter_date as (
  select distinct old_encounter_id
  ,start_date as encounter_start_date
  from {{ ref('lab__generate_encounter_id') }}
)

,detail_values as (
    select stg.*
    ,cli.encounter_id
    ,cli.old_encounter_id
    ,cli.encounter_type
    ,d.encounter_start_date 
    , row_number() over (partition by cli.encounter_id order by stg.claim_type, stg.start_date) as encounter_row_number --institutional then professional
    from  {{ ref('encounters__stg_medical_claim') }} stg
    inner join {{ ref('encounters__combined_claim_line_crosswalk') }} cli on stg.claim_id = cli.claim_id  
    and
    stg.claim_line_number = cli.claim_line_number
    and
    cli.encounter_type = 'lab'
    and
    cli.claim_line_attribution_number = 1
    inner join encounter_date d on cli.old_encounter_id = d.old_encounter_id
)

, patient as (
    select 
        patient_id
        , birth_date
        , gender
        , race
    from {{ ref('encounters__stg_eligibility') }}
    where patient_row_num = 1
    )

, total_amounts as (
    select 
    encounter_id
    , encounter_type
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
    , count(distinct claim_id) as claim_count
    , count(distinct(case when claim_type = 'institutional' then claim_id else null end))  as inst_claim_count
    , count(distinct(case when claim_type = 'professional' then claim_id else null end))  as prof_claim_count
from detail_values
group by encounter_id
,encounter_type -- not changing grain, but bringing into final
)


,highest_paid_diagnosis as 
(
  select encounter_id
  , diagnosis_code_1
  , diagnosis_code_type
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where diagnosis_code_1 is not null
  group by diagnosis_code_1
  , encounter_id
  , diagnosis_code_type
)

,highest_paid_facility as 
(
  select encounter_id
  , facility_id
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where facility_id is not null
  group by 
   encounter_id
  , facility_id
)

,highest_paid_pos as 
(
  select encounter_id
  , place_of_service_code
  , place_of_service_description
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where place_of_service_code is not null
  group by 
   encounter_id
  , place_of_service_code
  , place_of_service_description
)

select   d.encounter_id
, d.encounter_start_date
, d.patient_id
, tot.encounter_type
, {{ dbt.datediff("birth_date","d.encounter_start_date","day")}}/365 as admit_age
, e.gender
, e.race
, hp.diagnosis_code_type as primary_diagnosis_code_type
, hp.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
, hf.facility_id as facility_id
, b.provider_organization_name as facility_name
, b.primary_specialty_description as facility_type
, pos.place_of_service_code
, pos.place_of_service_description
, tot.total_paid_amount
, tot.total_allowed_amount
, tot.total_charge_amount
, tot.claim_count
, tot.inst_claim_count
, tot.prof_claim_count
, d.data_source
, '{{ var('tuva_last_run')}}' as tuva_last_run
from detail_values d
inner join total_amounts tot on d.encounter_id = tot.encounter_id
left join highest_paid_diagnosis hp on d.encounter_id = hp.encounter_id
and
hp.paid_order = 1
left join highest_paid_facility hf on d.encounter_id = hf.encounter_id
and
hf.paid_order = 1
left join highest_paid_pos pos on d.encounter_id = pos.encounter_id
and
pos.paid_order = 1
left join patient e
  on d.patient_id = e.patient_id
left join dev_brad.terminology.provider b
  on hf.facility_id = b.npi
left join dev_brad.terminology.icd_10_cm icd10cm
  on hp.diagnosis_code_1 = icd10cm.icd_10_cm
  and hp.diagnosis_code_type = 'icd-10-cm'
left join dev_brad.terminology.icd_9_cm icd9cm
  on hp.diagnosis_code_1 = icd9cm.icd_9_cm
  and hp.diagnosis_code_type = 'icd-9-cm'
where d.encounter_row_number = 1