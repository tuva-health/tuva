{{ config(
    enabled = var('claims_enabled', False)
) }}

with all_prof_aip_claim_lines as (

    select
          claim_id
        , person_id
        , place_of_service_code
        , paid_amount
    from {{ ref('medical_claim') }}
    where place_of_service_code = '21'

)

,  group_at_claim_grain as (

    select
          claim_id
        , max(person_id) as person_id
        , sum(paid_amount) as paid_amount
    from all_prof_aip_claim_lines
    group by
          claim_id

)

,  add_other_fields as (

    select
          aa.claim_id
        , aa.person_id
        , aa.paid_amount
        , bb.usable_person_id
        , cc.merge_start_date
        , cc.merge_end_date
        , cc.usable_merge_dates
    from group_at_claim_grain aa
    left join {{ ref('data_quality__other_header_values') }} bb
      on aa.claim_id = bb.claim_id
      and aa.person_id = bb.person_id
    left join {{ ref('data_quality__claim_grain_calculated_dates') }} cc
      on aa.claim_id = cc.claim_id

)

,  add_usable_flag as (

    select
          claim_id
        , person_id
        , paid_amount
        , usable_person_id
        , merge_start_date
        , merge_end_date
        , usable_merge_dates
        , case
            when (usable_person_id = 1 and usable_merge_dates = 1) then 1
            else 0
          end as usable_prof_claim
    from add_other_fields

)

select
      claim_id
    , person_id
    , paid_amount
    , usable_person_id
    , merge_start_date
    , merge_end_date
    , usable_merge_dates
    , usable_prof_claim
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_usable_flag
