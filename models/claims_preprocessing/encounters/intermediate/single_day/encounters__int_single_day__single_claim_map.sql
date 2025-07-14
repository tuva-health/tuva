-- TODO: Need to revisit ASC. The current OS logic is different for ASC.
with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
)
select
    medical_claim_sk
    , service_category_2
    , claim_type
    , case when substring(hcpcs_code, 1, 1) = 'J' then 1 else 0 end as injection_flag
    , {{ dbt_utils.generate_surrogate_key(['service_category_2', 'patient_sk', 'start_date']) }} as encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 1 as claim_priority
from encounters__stg_medical_claim
where service_category_2 in (
    'ambulance'
    , 'ambulatory surgery center'
    , 'dialysis'
    , 'durable medical equipment'
    , 'home health'
    , 'lab'
    , 'observation'
    , 'outpatient hospice'
    , 'outpatient hospital or clinic'
    , 'outpatient psychiatric'
    , 'outpatient pt/ot/st'
    , 'outpatient radiology'
    , 'outpatient rehabilitation'
    , 'outpatient substance use'
    , 'outpatient surgery'
    , 'urgent care'
    )