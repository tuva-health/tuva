with encounters__stg_medical_claim as (
    select *, 'other' as method -- this is added to distinguish these from office visit encounters
    from {{ ref('encounters__stg_medical_claim') }}
)
select
    medical_claim_sk
    , case
        when substring(hcpcs_code, 1, 1) = 'J' then 'outpatient injections'
        else service_category_2 end as encounter_type
    , claim_type
    , -- Generated ID = method, patient, date, facility
    {{ dbt_utils.generate_surrogate_key(['method', 'patient_sk', 'start_date', 'facility_npi']) }} as encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 1 as claim_priority
from encounters__stg_medical_claim
where service_category_2 in (
    'ambulance'
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
    /*
    TODO:
    if there are two claims that happen on the same day and have different facility_id, those are two
    encounters that happened on the same day in different places. I don't know if that's possible in
    the real world or if when we see that it's because of a data quality problem. But when we see that,
    we can't roll up any professional or ancillary claims from that day to either of those encounters
    because we don't know which of the two encounters to roll them up to, so I think we need to somehow
    label those professional or ancillary claims as claims that can't be rolled up to an encounter
    because it is not possible to decide which encounter they belong to.
    */