with encounters__stg_medical_claim as (
    select *
    from {{ ref('encounters__stg_medical_claim') }}
),
encounters__int_crosswalk__claim_encounter as (
    select *
    from {{ ref('encounters__int_crosswalk__claim_encounter') }}
)

-- Get claims without encounters
select
    med.medical_claim_sk
    , {{ dbt_utils.generate_surrogate_key(['data_source', 'claim_id']) }} as encounter_id
    , 'orphaned claim' as encounter_type
    , 'other' as encounter_group
from encounters__stg_medical_claim as med
    left outer join encounters__int_crosswalk__claim_encounter as enc
    on med.medical_claim_sk = enc.medical_claim_sk
where enc.medical_claim_sk is null