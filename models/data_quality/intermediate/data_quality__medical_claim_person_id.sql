{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with medical as (
  select
      claim_id
    , count(distinct p.person_id) as person_id_count
    , max(case when p.person_id is null then 1 else 0 end) as missing_person_id
    , max(
        case 
          when startdts.month_start_date is null then 1
          when enddts.month_start_date is null then 1
          else 0
        end
      ) as missing_eligibility
  from {{ ref('input_layer__medical_claim') }} p
  left join {{ ref('data_quality__eligibility_dq_stage') }} startdts
    on p.person_id = startdts.person_id
    and p.claim_start_date between startdts.month_start_date and startdts.month_end_date
  left join {{ ref('data_quality__eligibility_dq_stage') }} enddts
    on p.person_id = enddts.person_id
    and p.claim_end_date between enddts.month_start_date and enddts.month_end_date
  group by
      claim_id
)

, final as (
  select
      'multiple medical_claim person_ids' as data_quality_check
    , sum(case when person_id_count > 1 then 1 else 0 end) as result_count
  from medical

  union all

  select
      'missing medical_claim person_id' as data_quality_check
    , sum(missing_person_id) as result_count
  from medical

  union all

  select
      'orphaned medical_claim claims' as data_quality_check
    , sum(missing_eligibility) as result_count
  from medical
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
