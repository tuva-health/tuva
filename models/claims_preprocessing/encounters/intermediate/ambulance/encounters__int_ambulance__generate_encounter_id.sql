with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
)
select
    medical_claim_sk
    , {{ dbt_utils.generate_surrogate_key(['data_source', 'member_id', 'start_date']) }} as encounter_id
    , start_date
    , end_date
from encounters__stg_medical_claim
where service_category_2 = 'ambulance' --both inst and prof
