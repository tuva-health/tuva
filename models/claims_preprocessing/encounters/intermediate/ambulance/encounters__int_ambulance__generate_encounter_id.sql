with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
),
anchor as (
    select distinct
        patient_data_source_id
        , claim_id
        , start_date
    from encounters__stg_medical_claim
    where service_category_2 = 'ambulance' --both inst and prof
)
select
    patient_data_source_id
    , claim_id
    , start_date
    , dense_rank() over (order by patient_data_source_id, start_date) as old_encounter_id
from anchor as a
