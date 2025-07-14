with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
)
, encounters__int_claim_encounter_crosswalk as (
    select *
    from {{ ref('encounters__int_claim_encounter_crosswalk') }}
)
, encounters__stg_patient as (
    select *
    from {{ ref('encounters__stg_patient') }}
)
, base_claims as (
    select
        stg.*
        , cex.encounter_sk
        , cex.encounter_type
        , cex.encounter_group
        , cex.encounter_start_date
        , cex.encounter_end_date
        , row_number() over (partition by cex.encounter_sk
            order by stg.claim_type, stg.start_date) as encounter_row_number
    from encounters__stg_medical_claim as stg
        inner join encounters__int_claim_encounter_crosswalk as cex
        on cex.medical_claim_sk = stg.medical_claim_sk
        and cex.encounter_group = 'office based'
        and cex.encounter_type_priority = 1
)

, highest_paid_diagnosis as (
    select encounter_sk
        , primary_diagnosis_code
        , primary_diagnosis_code_type
        , primary_diagnosis_description
        , row_number() over (partition by encounter_sk order by sum(paid_amount) desc) as paid_order
        , sum(paid_amount) as paid_amount
    from base_claims
    where primary_diagnosis_code is not null
    group by encounter_sk
        , primary_diagnosis_code
        , primary_diagnosis_code_type
        , primary_diagnosis_description
)

, highest_paid_facility as (
    select encounter_sk
        , facility_npi
        , facility_name
        , facility_type
        , row_number() over (partition by encounter_sk order by sum(paid_amount) desc) as paid_order
        , sum(paid_amount) as paid_amount
    from base_claims
    where facility_npi is not null
    group by
        encounter_sk
        , facility_npi
        , facility_name
        , facility_type
)

, highest_paid_physician as (
    select 
        encounter_sk
        , billing_npi
        , row_number() over (partition by encounter_sk order by sum(paid_amount) desc) as paid_order
        , sum(paid_amount) as paid_amount
    from base_claims
    where billing_npi is not null
    group by
        encounter_sk
        , billing_npi
)

, highest_paid_hcpc as (
    select 
        encounter_sk
        , hcpcs_code
        , ccs_category
        , ccs_category_description
        , row_number() over (partition by encounter_sk order by sum(paid_amount) desc) as paid_order
        , sum(paid_amount) as paid_amount
    from base_claims
    where hcpcs_code is not null
    group by
        encounter_sk
        , hcpcs_code
        , ccs_category
        , ccs_category_description
)
, service_category_flags as (
    select
        encounter_sk
        , max(case when service_category_3 in ('l/d - cesarean delivery', 'l/d - vaginal delivery') then 1 else 0 end) as delivery_flag
        , max(case when service_category_3 = 'l/d - cesarean delivery' then 1 else 0 end) as cesarean_delivery
        , max(case when service_category_3 = 'l/d - vaginal delivery' then 1 else 0 end) as vaginal_delivery
        , max(case when service_category_3 in ('l/d - newborn', 'l/d - newborn nicu') then 1 else 0 end) as newborn_flag
        , max(case when service_category_3 = 'l/d - newborn nicu' then 1 else 0 end) as nicu_flag
        , max(case when service_category_2 = 'observation' then 1 else 0 end) as observation_flag
        , max(case when service_category_2 = 'emergency department' then 1 else 0 end) as ed_flag
        , max(case when service_category_2 = 'lab' then 1 else 0 end) as lab_flag
        , max(case when service_category_2 = 'ambulance' then 1 else 0 end) as ambulance_flag
        , max(case when service_category_2 = 'durable medical equipment' then 1 else 0 end) as dme_flag
        , max(case when service_category_2 in ('pharmacy', 'outpatient pharmacy', 'office-based pharmacy') then 1 else 0 end) as pharmacy_flag
    from base_claims
    group by encounter_sk
)

, total_amounts as (
    select
        encounter_sk
        , sum(paid_amount) as paid_amount
        , sum(allowed_amount) as allowed_amount
        , sum(charge_amount) as charge_amount
        , count(distinct claim_id) as claim_count
        , count(distinct(case when claim_type = 'institutional' then claim_id else null end)) as inst_claim_count
        , count(distinct(case when claim_type = 'professional' then claim_id else null end)) as prof_claim_count
    from base_claims
    group by encounter_sk
)

select
    cast('tuva' as {{ dbt.type_string() }}) as data_source
    , a.encounter_sk
    , a.member_id
    , a.patient_sk
    , a.encounter_type
    , a.encounter_group
    , a.encounter_start_date
    , a.encounter_end_date
    , cast(null as {{ dbt.type_string() }}) as admit_source_code
    , cast(null as {{ dbt.type_string() }}) as admit_source_description
    , cast(null as {{ dbt.type_string() }}) as admit_type_code
    , cast(null as {{ dbt.type_string() }}) as admit_type_description
    , cast(null as {{ dbt.type_string() }}) as discharge_disposition_code
    , cast(null as {{ dbt.type_string() }}) as discharge_disposition_description
    , cast(null as {{ dbt.type_string() }}) as attending_provider_id
    , cast(null as {{ dbt.type_string() }}) as attending_provider_name
    , hf.facility_npi as facility_id
    , hf.facility_name
    , hf.facility_type
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
    , hp.primary_diagnosis_code_type
    , hp.primary_diagnosis_code
    , hp.primary_diagnosis_description
    , cast(null as {{ dbt.type_string() }}) as drg_code_type
    , cast(null as {{ dbt.type_string() }}) as drg_code
    , cast(null as {{ dbt.type_string() }}) as drg_description
    , tot.paid_amount
    , tot.allowed_amount
    , tot.charge_amount
    , tot.claim_count
    , tot.inst_claim_count
    , tot.prof_claim_count
    , {{ calculate_age("p.birth_date","a.encounter_start_date") }} as admit_age
--    , phy.billing_npi
--    , hcpc.hcpcs_code
--    , hcpc.ccs_category
--    , hcpc.ccs_category_description
from base_claims as a
    inner join total_amounts as tot 
    on a.encounter_sk = tot.encounter_sk
    inner join service_category_flags as sc 
    on a.encounter_sk = sc.encounter_sk
    left outer join highest_paid_diagnosis as hp 
    on a.encounter_sk = hp.encounter_sk
    and hp.paid_order = 1
    left outer join highest_paid_facility as hf 
    on a.encounter_sk = hf.encounter_sk
    and hf.paid_order = 1
    left outer join highest_paid_physician as phy 
    on a.encounter_sk = phy.encounter_sk
    and phy.paid_order = 1
    left outer join highest_paid_hcpc as hcpc 
    on a.encounter_sk = hcpc.encounter_sk
    and hcpc.paid_order = 1
    left outer join encounters__stg_patient as p
    on a.patient_sk = p.patient_sk
where a.encounter_row_number = 1
