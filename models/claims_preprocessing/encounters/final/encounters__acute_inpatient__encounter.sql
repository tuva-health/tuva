with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
)
, encounters__int_claim_encounter_crosswalk as (
    select *
    from {{ ref('encounters__int_claim_encounter_crosswalk') }}
)
, encounters__int_acute_inpatient__start_end_dates as (
    select *
    from {{ ref('encounters__int_acute_inpatient__start_end_dates') }}
)
, encounters__stg_eligibility as (
    select *
    from {{ ref('encounters__stg_eligibility') }}
)
, base_claims as (
    select
        stg.*
        , cex.encounter_sk
        , cex.encounter_type
        , cex.encounter_group
        , dts.encounter_start_date
        , dts.encounter_end_date
    from encounters__stg_medical_claim as stg
        inner join encounters__int_claim_encounter_crosswalk as cex
        on cex.medical_claim_sk = stg.medical_claim_sk
        and cex.encounter_type = 'acute inpatient'
        and cex.encounter_type_priority = 1
        inner join encounters__int_acute_inpatient__start_end_dates as dts
        on cex.encounter_id = dts.encounter_id
)

, patient as (
    select data_source
        , member_id
        , birth_date
        , gender
        , race
    from encounters__stg_eligibility
)

, encounter as (
    select distinct
        encounter_sk
        , encounter_type
        , encounter_group
        , encounter_start_date
        , encounter_end_date
    from base_claims
)

, claims_sequenced as (
    select *
        , row_number() over (partition by encounter_sk order by encounter_start_date, claim_id) as first_num
        , row_number() over (partition by encounter_sk order by encounter_end_date desc, claim_id) as last_num
    from base_claims
    where claim_type = 'institutional'
)

, institutional_claim_details as (
    select distinct
        c.data_source
        , c.encounter_sk
        , c.member_id
        , f.primary_diagnosis_code
        , f.primary_diagnosis_code_type
        , f.primary_diagnosis_description
        , f.facility_npi
        , f.facility_name
        , f.facility_type
        , f.drg_code_type
        , f.drg_code
        , f.drg_description
        , f.medical_surgical
        , f.admit_source_code
        , f.admit_source_description
        , f.admit_type_code
        , f.admit_type_description
        , l.discharge_disposition_code
        , l.discharge_disposition_description
    from base_claims as c
        inner join claims_sequenced as f
        on c.encounter_sk = f.encounter_sk
        and f.first_num = 1
        inner join claims_sequenced as l
        on c.encounter_sk = l.encounter_sk
        and l.last_num = 1
)

, service_category_flags as (
    select
        d.encounter_sk
        , max(case when d.service_category_3 in ('l/d - cesarean delivery', 'l/d - vaginal delivery') then 1 else 0 end) as delivery_flag
        , max(case when d.service_category_3 = 'l/d - cesarean delivery' then 1 else 0 end) as cesarean_delivery
        , max(case when d.service_category_3 = 'l/d - vaginal delivery' then 1 else 0 end) as vaginal_delivery
        , max(case when d.service_category_3 in ('l/d - newborn', 'l/d - newborn nicu') then 1 else 0 end) as newborn_flag
        , max(case when d.service_category_3 = 'l/d - newborn nicu' then 1 else 0 end) as nicu_flag
        , max(case when d.service_category_2 = 'observation' then 1 else 0 end) as observation_flag
        , max(case when d.service_category_2 = 'emergency department' then 1 else 0 end) as ed_flag
        , max(case when d.service_category_2 = 'lab' then 1 else 0 end) as lab_flag
        , max(case when d.service_category_2 = 'ambulance' then 1 else 0 end) as ambulance_flag
        , max(case when d.service_category_2 = 'durable medical equipment' then 1 else 0 end) as dme_flag
        , max(case when d.service_category_2 = 'pharmacy' then 1 else 0 end) as pharmacy_flag
    from base_claims as d
    group by d.encounter_sk
)

, total_amounts as (
    select
        encounter_sk
        , sum(paid_amount) as paid_amount
        , sum(allowed_amount) as allowed_amount
        , sum(charge_amount) as charge_amount
        , count(distinct claim_id) as claim_count
        , count(distinct(case when claim_type = 'institutional' then concat(claim_id, ',', data_source) else null end)) as inst_claim_count
        , count(distinct(case when claim_type = 'professional' then concat(claim_id, ',', data_source) else null end)) as prof_claim_count
    from base_claims
    group by encounter_sk
)

select
    cast('tuva' as {{ dbt.type_string() }}) as data_source
    , x.encounter_sk
    , c.member_id
    , x.encounter_type
    , x.encounter_group
    , x.encounter_start_date
    , x.encounter_end_date
    , c.admit_source_code
    , c.admit_source_description
    , c.admit_type_code
    , c.admit_type_description
    , c.discharge_disposition_code
    , c.discharge_disposition_description
    , cast(null as {{ dbt.type_string() }}) as attending_provider_id
    , cast(null as {{ dbt.type_string() }}) as attending_provider_name
    , c.facility_npi as facility_id
    , c.facility_name
    , c.facility_type
    , sc.observation_flag
    , sc.lab_flag
    , sc.dme_flag
    , sc.ambulance_flag
    , sc.pharmacy_flag
    , sc.ed_flag
    , sc.delivery_flag
    , case
        when sc.cesarean_delivery = 1 then 'cesarean'
        when sc.vaginal_delivery = 1 then 'vaginal'
        else null end as delivery_type
    , sc.newborn_flag
    , sc.nicu_flag
    , c.primary_diagnosis_code_type
    , c.primary_diagnosis_code
    , c.primary_diagnosis_description
    , c.drg_code_type
    , c.drg_code
    , c.drg_description
    , tot.paid_amount
    , tot.allowed_amount
    , tot.charge_amount
    , tot.claim_count
    , tot.inst_claim_count
    , tot.prof_claim_count
    , {{ calculate_age("p.birth_date","x.encounter_start_date") }} as admit_age
--    , p.gender
--    , p.race
--    , c.medical_surgical
from encounter as x
    inner join total_amounts as tot
    on x.encounter_sk = tot.encounter_sk
    inner join service_category_flags as sc
    on x.encounter_sk = sc.encounter_sk
    inner join institutional_claim_details as c
    on x.encounter_sk = c.encounter_sk
    left outer join patient as p
    on c.member_id = p.member_id
    and c.data_source = p.data_source
