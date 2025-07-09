with orphan_claims as (
    select
        med.medical_claim_sk
        , med.patient_data_source_id
        , med.claim_id
    from {{ ref('encounters__stg_medical_claim') }} as med
        left outer join {{ ref('encounters__int_combined_claim_line_crosswalk') }} as enc
        on med.medical_claim_sk = enc.medical_claim_sk
    where enc.medical_claim_sk is null
)

, max_encounter as (
    select max(encounter_id) as max_encounter_id
    from {{ ref('encounters__int_combined_claim_line_crosswalk') }}
)

select
    medical_claim_sk
    , dense_rank() over (order by patient_data_source_id, claim_id)
        + max_encounter.max_encounter_id as encounter_id
    , 'orphaned claim' as encounter_type
    , 'other' as encounter_group
from orphan_claims
cross join max_encounter
