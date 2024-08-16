{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}



with detail_values as (
    select stg.*
    ,cli.encounter_id
    ,cli.old_encounter_id
    ,cli.encounter_type
    ,ed.encounter_start_date 
    ,ed.encounter_end_date 
    , row_number() over (partition by cli.encounter_id order by stg.claim_type, stg.start_date) as encounter_row_number --institutional then professional
    from  {{ ref('encounters__stg_medical_claim') }} stg
    inner join {{ ref('encounters__combined_claim_line_crosswalk') }} cli on stg.claim_id = cli.claim_id  
    and
    stg.claim_line_number = cli.claim_line_number
    and
    cli.encounter_type = 'ambulatory surgery center'
    and
    cli.claim_line_attribution_number = 1
    inner join {{ ref('asc__start_end_dates') }} ed on cli.old_encounter_id = ed.old_encounter_id
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

,highest_paid_hcpc as 
(
  select encounter_id
  , hcpcs_code
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where facility_id is not null
  group by 
   encounter_id
  , hcpcs_code
)

select   d.encounter_id
, d.encounter_start_date
, d.encounter_end_date
, d.patient_id
, tot.encounter_type
, {{ dbt.datediff("birth_date","d.encounter_start_date","day")}}/365 as admit_age
, {{ dbt.datediff("d.encounter_start_date","d.encounter_end_date","day")}} as length_of_stay
, e.gender
, e.race
, hp.diagnosis_code_type as primary_diagnosis_code_type
, hp.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
, hf.facility_id as facility_id
, b.provider_organization_name as facility_name
, hcpc.hcpcs_code
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
left join highest_paid_hcpc hcpc on d.encounter_id = hcpc.encounter_id
and
hf.paid_order = 1
left join patient e
  on d.patient_id = e.patient_id
left join {{ ref('terminology__provider') }} b
  on hf.facility_id = b.npi
left join {{ ref('terminology__icd_10_cm')}} icd10cm
  on hp.diagnosis_code_1 = icd10cm.icd_10_cm
  and hp.diagnosis_code_type = 'icd-10-cm'
left join {{ ref('terminology__icd_9_cm')}} icd9cm
  on hp.diagnosis_code_1 = icd9cm.icd_9_cm
  and hp.diagnosis_code_type = 'icd-9-cm'
where d.encounter_row_number = 1