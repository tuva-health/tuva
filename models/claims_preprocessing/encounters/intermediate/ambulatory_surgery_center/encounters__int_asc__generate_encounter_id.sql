with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
)
select
    medical_claim_sk
    , {{ dbt_utils.generate_surrogate_key(['patient_sk', 'start_date']) }} as encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
from encounters__stg_medical_claim
where service_category_2 = 'ambulatory surgery center' --both inst and prof
