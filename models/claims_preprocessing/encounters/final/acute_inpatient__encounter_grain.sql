{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with detail_values as (
    select stg.*
    , cli.encounter_id
    , cli.old_encounter_id
    , ed.encounter_start_date
    , ed.encounter_end_date
    , cli.encounter_type
    , cli.encounter_group
    from {{ ref('encounters__stg_medical_claim') }} as stg
    inner join {{ ref('encounters__combined_claim_line_crosswalk') }} as cli on stg.claim_id = cli.claim_id  --replace this ref with the deduped version when complete
    and
    stg.claim_line_number = cli.claim_line_number
    and
    cli.encounter_type = 'acute inpatient'
    and
    cli.claim_line_attribution_number = 1
    inner join {{ ref('acute_inpatient__start_end_dates') }} as ed on cli.old_encounter_id = ed.encounter_id
)

, encounter_cross_walk as (
  select distinct encounter_id
  , old_encounter_id
  from detail_values
)

, first_last_inst_inst_values as (
select *
, row_number() over (partition by encounter_id
order by start_date, claim_id) as first_num
, row_number() over (partition by encounter_id
order by end_date desc, claim_id) as last_num
from detail_values
where claim_type = 'institutional'
)

, institutional_claim_details as (
    select distinct
        d.encounter_id
        , f.diagnosis_code_1
        , f.diagnosis_code_type
        , f.facility_id as facility_id
        , f.drg_code_type
        , f.drg_code
        , f.admit_source_code as admit_source_code
        , f.admit_type_code as admit_type_code
        , l.discharge_disposition_code as discharge_disposition_code
        , d.patient_data_source_id
        , d.data_source
    from detail_values as d
    inner join first_last_inst_inst_values as f on d.encounter_id = f.encounter_id
    and
    f.first_num = 1
    inner join first_last_inst_inst_values as l on d.encounter_id = l.encounter_id
    and
    l.last_num = 1
)

, service_category_flags as (
    select
        d.encounter_id
       , max(case when d.service_category_3 in ('l/d - cesarean delivery', 'l/d - vaginal delivery') then 1 else 0 end) as delivery_flag
       , max(case when d.service_category_3 = 'l/d - cesarean delivery' then 1 else 0 end) as cesarean_delivery
       , max(case when d.service_category_3 = 'l/d - vaginal delivery' then 1 else 0 end) as vaginal_delivery
       , max(case when d.service_category_3 in ('l/d - newborn', 'l/d - newborn nicu') then 1 else 0 end) as newborn_flag
       , max(case when d.service_category_3 = 'l/d - newborn nicu' then 1 else 0 end) as nicu_flag
       , max(case when scr.service_category_2 = 'observation' then 1 else 0 end) as observation_flag
       , max(case when scr.service_category_2 = 'emergency department' then 1 else 0 end) as ed_flag
       , max(case when scr.service_category_2 = 'lab' then 1 else 0 end) as lab_flag
       , max(case when scr.service_category_2 = 'ambulance' then 1 else 0 end) as ambulance_flag
       , max(case when scr.service_category_2 = 'durable medical equipment' then 1 else 0 end) as dme_flag
       , max(case when scr.service_category_2 = 'pharmacy' then 1
              else 0 end) as pharmacy_flag
    from detail_values as d
    left outer join {{ ref('service_category__service_category_grouper') }} as scr on d.claim_id = scr.claim_id
    and
    scr.claim_line_number = d.claim_line_number
    group by d.encounter_id
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
    , encounter_type
    , encounter_group
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
    , count(distinct claim_id) as claim_count
    , count(distinct(case when claim_type = 'institutional' then claim_id else null end)) as inst_claim_count
    , count(distinct(case when claim_type = 'professional' then claim_id else null end)) as prof_claim_count
from detail_values
group by encounter_id
, encounter_type -- not changing grain, but bringing into final
, encounter_group
)



select
  x.encounter_id
, a.encounter_start_date
, a.encounter_end_date
, c.patient_data_source_id
, tot.encounter_type
, tot.encounter_group
, {{ dbt.datediff("birth_date","encounter_end_date","day") }} / 365 as admit_age
, e.gender
, e.race
, c.diagnosis_code_type as primary_diagnosis_code_type
, c.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
, c.facility_id as facility_id
, sc.observation_flag
, sc.ed_flag
, sc.lab_flag
, sc.dme_flag
, sc.ambulance_flag
, sc.pharmacy_flag
, b.provider_organization_name as facility_name
, b.primary_specialty_description as facility_type
, sc.delivery_flag
, case when sc.cesarean_delivery = 1 then 'cesarean'
       when sc.vaginal_delivery = 1 then 'vaginal'
       else null end as delivery_type
, sc.newborn_flag
, sc.nicu_flag
, c.drg_code_type
, c.drg_code
, coalesce(msdrg.ms_drg_description, aprdrg.apr_drg_description) as drg_description
, coalesce(msdrg.medical_surgical, aprdrg.medical_surgical) as medical_surgical
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
  end as mortality_flag
, c.data_source
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('acute_inpatient__start_end_dates') }} as a
inner join encounter_cross_walk as x on a.encounter_id = x.old_encounter_id
inner join total_amounts as tot on x.encounter_id = tot.encounter_id
inner join service_category_flags as sc on x.encounter_id = sc.encounter_id
left outer join institutional_claim_details as c
  on x.encounter_id = c.encounter_id
left outer join patient as e
  on c.patient_data_source_id = e.patient_data_source_id
left outer join {{ ref('terminology__provider') }} as b
  on c.facility_id = b.npi
left outer join {{ ref('terminology__discharge_disposition') }} as g
  on c.discharge_disposition_code = g.discharge_disposition_code
left outer join {{ ref('terminology__admit_source') }} as h
  on c.admit_source_code = h.admit_source_code
left outer join {{ ref('terminology__admit_type') }} as i
  on c.admit_type_code = i.admit_type_code
left outer join {{ ref('terminology__ms_drg') }} as msdrg
  on c.drg_code_type = 'ms-drg'
  and c.drg_code = msdrg.ms_drg_code
left outer join {{ ref('terminology__apr_drg') }} as aprdrg
  on c.drg_code_type = 'apr-drg'
  and c.drg_code = aprdrg.apr_drg_code
left outer join {{ ref('terminology__icd_10_cm') }} as icd10cm
  on c.diagnosis_code_1 = icd10cm.icd_10_cm
  and c.diagnosis_code_type = 'icd-10-cm'
left outer join {{ ref('terminology__icd_9_cm') }} as icd9cm
  on c.diagnosis_code_1 = icd9cm.icd_9_cm
  and c.diagnosis_code_type = 'icd-9-cm'
