{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}



 with patient as (
    select
        patient_data_source_id
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
    , encounter_group
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
    , count(distinct claim_id) as claim_count
    , count(distinct(case when claim_type = 'institutional' then claim_id else null end)) as inst_claim_count
    , count(distinct(case when claim_type = 'professional' then claim_id else null end)) as prof_claim_count
from {{ ref('office_visits__int_detail_values') }}
group by encounter_id
, encounter_type
, encounter_group
)


, highest_paid_diagnosis as (
  select encounter_id
  , diagnosis_code_1
  , diagnosis_code_type
  , row_number() over (partition by encounter_id
order by sum(paid_amount) desc) as paid_order
  , sum(paid_amount) as paid_amount
  from {{ ref('office_visits__int_detail_values') }}
  where diagnosis_code_1 is not null
  group by diagnosis_code_1
  , encounter_id
  , diagnosis_code_type
)

, highest_paid_facility as (
  select encounter_id
  , facility_id
  , row_number() over (partition by encounter_id
order by sum(paid_amount) desc) as paid_order
  , sum(paid_amount) as paid_amount
  from {{ ref('office_visits__int_detail_values') }}
  where facility_id is not null
  group by
   encounter_id
  , facility_id
)

, highest_paid_physician as (
  select encounter_id
  , billing_id
  , row_number() over (partition by encounter_id
order by sum(paid_amount) desc) as paid_order
  , sum(paid_amount) as paid_amount
  from {{ ref('office_visits__int_detail_values') }}
  where billing_id is not null
  group by
   encounter_id
  , billing_id
)

, highest_paid_hcpc as (
  select encounter_id
  , hcpcs_code
  , ccs_category
  , ccs_category_description
  , row_number() over (partition by encounter_id
order by sum(paid_amount) desc) as paid_order
  , sum(paid_amount) as paid_amount
  from {{ ref('office_visits__int_detail_values') }}
  where hcpcs_code is not null
  group by
   encounter_id
  , hcpcs_code
  , ccs_category
  , ccs_category_description
)



, service_category_flags as (
    select
        d.encounter_id
       , max(case when scr.service_category_2 = 'lab' then 1 else 0 end) as lab_flag
       , max(case when scr.service_category_2 = 'ambulance' then 1 else 0 end) as ambulance_flag
       , max(case when scr.service_category_2 = 'durable medical equipment' then 1 else 0 end) as dme_flag
       , max(case when scr.service_category_2 = 'observation' then 1 else 0 end) as observation_flag
       , max(case when scr.service_category_2 = 'pharmacy' then 1
              else 0 end) as pharmacy_flag
    from {{ ref('office_visits__int_detail_values') }} as d
    left outer join {{ ref('service_category__service_category_grouper') }} as scr on d.claim_id = scr.claim_id
    and
    scr.claim_line_number = d.claim_line_number
    group by d.encounter_id
)


select d.encounter_id
, d.encounter_start_date
, d.patient_data_source_id
, tot.encounter_type
, tot.encounter_group
, {{ dbt.datediff("birth_date","d.encounter_start_date","day") }} / 365 as admit_age
, e.gender
, e.race
, hp.diagnosis_code_type as primary_diagnosis_code_type
, hp.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
, hf.facility_id as facility_id
, b.provider_organization_name as facility_name
, phy.billing_id
, {{ concat_custom(["b2.provider_first_name", "' '", "b2.provider_last_name"]) }} as provider_name
, b2.primary_specialty_description as provider_specialty
, sc.lab_flag
, sc.dme_flag
, sc.ambulance_flag
, sc.pharmacy_flag
, hcpc.hcpcs_code
, hcpc.ccs_category
, hcpc.ccs_category_description
, tot.total_paid_amount
, tot.total_allowed_amount
, tot.total_charge_amount
, tot.claim_count
, tot.inst_claim_count
, tot.prof_claim_count
, d.data_source
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('office_visits__int_detail_values') }} as d
inner join total_amounts as tot on d.encounter_id = tot.encounter_id
inner join service_category_flags as sc on d.encounter_id = sc.encounter_id
left outer join highest_paid_diagnosis as hp on d.encounter_id = hp.encounter_id
and
hp.paid_order = 1
left outer join highest_paid_facility as hf on d.encounter_id = hf.encounter_id
and
hf.paid_order = 1
left outer join highest_paid_physician as phy on d.encounter_id = phy.encounter_id
and
phy.paid_order = 1
left outer join highest_paid_hcpc as hcpc on d.encounter_id = hcpc.encounter_id
and
hcpc.paid_order = 1
left outer join patient as e
  on d.patient_data_source_id = e.patient_data_source_id
left outer join {{ ref('terminology__provider') }} as b
  on hf.facility_id = b.npi
left outer join {{ ref('terminology__provider') }} as b2
  on phy.billing_id = b2.npi
left outer join {{ ref('terminology__icd_10_cm') }} as icd10cm
  on hp.diagnosis_code_1 = icd10cm.icd_10_cm
  and hp.diagnosis_code_type = 'icd-10-cm'
left outer join {{ ref('terminology__icd_9_cm') }} as icd9cm
  on hp.diagnosis_code_1 = icd9cm.icd_9_cm
  and hp.diagnosis_code_type = 'icd-9-cm'
where d.encounter_row_number = 1
