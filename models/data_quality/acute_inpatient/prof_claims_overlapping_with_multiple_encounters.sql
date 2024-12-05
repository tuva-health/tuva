{{ config(
    enabled = var('claims_enabled', False)
) }}

with number_of_encounters_each_prof_claim_overlaps_with as (

    select
          claim_id
        , patient_id
        , count(distinct encounter_id) as encounters_claim_overlaps_with
    from {{ ref('prof_claims_overlapping_with_aip_encounters') }}
    group by
          claim_id
        , patient_id

)

, prof_claims_that_overlap_with_multiple_encounters as (

    select
          claim_id
        , patient_id
    from number_of_encounters_each_prof_claim_overlaps_with
    where encounters_claim_overlaps_with > 1

)

select
      bb.claim_id
    , bb.patient_id
    , bb.paid_amount
    , bb.usable_patient_id
    , bb.merge_start_date
    , bb.merge_end_date
    , bb.usable_merge_dates
    , bb.encounter_id
from prof_claims_that_overlap_with_multiple_encounters aa
inner join {{ ref('prof_claims_overlapping_with_aip_encounters') }} bb
    on aa.claim_id = bb.claim_id
        and aa.patient_id = bb.patient_id
