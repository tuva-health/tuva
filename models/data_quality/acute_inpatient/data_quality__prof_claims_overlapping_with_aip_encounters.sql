{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      aa.claim_id
    , aa.person_id
    , aa.paid_amount
    , aa.usable_person_id
    , aa.merge_start_date
    , aa.merge_end_date
    , aa.usable_merge_dates
    , aa.usable_prof_claim
    , bb.encounter_id
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__all_prof_aip_claims') }} aa
left join {{ ref('data_quality__aip_encounters_institutional_definition') }} bb
    on aa.person_id = bb.person_id
    and (
           aa.merge_start_date between bb.encounter_start_date and bb.encounter_end_date
        or aa.merge_end_date between bb.encounter_start_date and bb.encounter_end_date
        or bb.encounter_start_date between aa.merge_start_date and aa.merge_end_date
        or bb.encounter_end_date between aa.merge_start_date and aa.merge_end_date
    )
where aa.usable_prof_claim = 1
