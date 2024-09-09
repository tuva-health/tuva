{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with detail_values as (
    select stg.*
    ,cli.encounter_id
    ,cli.old_encounter_id
    ,ed.encounter_start_date
    ,ed.encounter_end_date
      ,cli.encounter_type
    ,cli.encounter_group
    from  {{ ref('encounters__stg_medical_claim') }} stg
    inner join {{ ref('encounters__combined_claim_line_crosswalk') }} cli on stg.claim_id = cli.claim_id  --replace this ref with the deduped version when complete
    and
    stg.claim_line_number = cli.claim_line_number
    and
    cli.encounter_type = 'inpatient hospice'
    and
    cli.claim_line_attribution_number = 1
    inner join {{ ref('inpatient_hospice__start_end_dates') }} ed on cli.old_encounter_id = ed.encounter_id
)

,encounter_cross_walk as (
  select distinct encounter_id
  ,old_encounter_id
  from detail_values
)

,first_last_inst_inst_values as (
select *
,row_number() over (partition by encounter_id order by start_date, claim_id) as first_num
,row_number() over (partition by encounter_id order by end_date desc, claim_id) as last_num
from detail_values
where claim_type = 'institutional'
)

, institutional_claim_details as (
    select distinct
        d.encounter_id
        , f.diagnosis_code_1
        , f.diagnosis_code_type
        , f.facility_id as facility_id
        , f.ms_drg_code as ms_drg_code
        , f.apr_drg_code as apr_drg_code
        , f.admit_source_code as admit_source_code
        , f.admit_type_code as admit_type_code
        , l.discharge_disposition_code as discharge_disposition_code
        , d.patient_data_source_id
        , d.data_source
    from detail_values d
    inner join first_last_inst_inst_values f on d.encounter_id = f.encounter_id
    and
    f.first_num =1 
    inner join first_last_inst_inst_values l on d.encounter_id = l.encounter_id
    and
    l.last_num = 1 

)

, patient as (
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
    ,encounter_type
    ,encounter_group
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
    , count(distinct claim_id) as claim_count
    , count(distinct(case when claim_type = 'institutional' then claim_id else null end))  as inst_claim_count
    , count(distinct(case when claim_type = 'professional' then claim_id else null end))  as prof_claim_count
from detail_values
group by encounter_id
,encounter_type 
, encounter_group

)

--bring in all service categories regardless of prioritization
, service_category_ranking as (
  select *
  from {{ ref('service_category__service_category_grouper') }}
  where service_category_2 in ('Observation','Emergency Department','Lab','Ambulance','Durable Medical Equipment','Pharmacy')
)

, service_category_flags as (
    select distinct
        d.encounter_id
       ,max(case when scr.service_category_2 = 'Lab' then 1 else 0 end) as lab_flag
       ,max(case when scr.service_category_2 = 'Ambulance' then 1 else 0 end) as ambulance_flag
       ,max(case when scr.service_category_2 = 'Durable Medical Equipment' then 1 else 0 end) as dme_flag
       ,max(case when scr.service_category_2 = 'Observation' then 1 else 0 end) as observation_flag
       ,max(case when scr.service_category_2 = 'Emergency Department' then 1 else 0 end) as ed_flag
       ,max(case when scr.service_category_2 = 'Pharmacy' then 1 else 0 end) as pharmacy_flag
    from detail_values d
    left join service_category_ranking scr on d.claim_id = scr.claim_id 
    and
    scr.claim_line_number = d.claim_line_number
    group by d.encounter_id
)

select
  x.encounter_id
, a.encounter_start_date
, a.encounter_end_date
, c.patient_data_source_id
,tot.encounter_type
,tot.encounter_group
, {{ dbt.datediff("birth_date","encounter_end_date","day")}}/365 as admit_age
, e.gender
, e.race
, c.diagnosis_code_type as primary_diagnosis_code_type
, c.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
, c.facility_id as facility_id
, b.provider_organization_name as facility_name
, b.primary_specialty_description as facility_type
, sc.lab_flag
, sc.dme_flag
, sc.ambulance_flag
, sc.pharmacy_flag
, sc.observation_flag
, sc.ed_flag
, c.ms_drg_code
, j.ms_drg_description
, j.medical_surgical
, c.apr_drg_code
, k.apr_drg_description
, c.admit_source_code
, h.admit_source_description
, c.admit_type_code
, i.admit_type_description
, c.discharge_disposition_code
, g.discharge_disposition_description
, tot.total_paid_amount
, tot.total_allowed_amount
, tot.total_charge_amount
, tot.claim_count
, tot.inst_claim_count
, tot.prof_claim_count
, {{ dbt.datediff("a.encounter_start_date","a.encounter_end_date","day") }} as length_of_stay
, case
    when c.discharge_disposition_code = '20' then 1
    else 0
  end mortality_flag
, c.data_source
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('inpatient_hospice__start_end_dates') }} a
inner join encounter_cross_walk x on a.encounter_id = x.old_encounter_id
inner join total_amounts tot on x.encounter_id = tot.encounter_id
inner join service_category_flags sc on x.encounter_id = sc.encounter_id
left join institutional_claim_details c
  on x.encounter_id = c.encounter_id
left join patient e
  on c.patient_data_source_id = e.patient_data_source_id
left join {{ ref('terminology__provider') }} b
  on c.facility_id = b.npi
left join {{ ref('terminology__discharge_disposition') }} g
  on c.discharge_disposition_code = g.discharge_disposition_code
left join {{ ref('terminology__admit_source') }} h
  on c.admit_source_code = h.admit_source_code
left join {{ ref('terminology__admit_type') }} i
  on c.admit_type_code = i.admit_type_code
left join {{ ref('terminology__ms_drg') }} j
  on c.ms_drg_code = j.ms_drg_code
left join {{ ref('terminology__apr_drg') }} k
  on c.apr_drg_code = k.apr_drg_code
left join {{ ref('terminology__icd_10_cm')}} icd10cm
  on c.diagnosis_code_1 = icd10cm.icd_10_cm
  and c.diagnosis_code_type = 'icd-10-cm'
left join {{ ref('terminology__icd_9_cm')}} icd9cm
  on c.diagnosis_code_1 = icd9cm.icd_9_cm
  and c.diagnosis_code_type = 'icd-9-cm'