{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with pharmacy as (
  select
      claim_id
    , count(distinct p.person_id) as person_id_count
    , max(case when p.person_id is null then 1 else 0 end) as missing_person_id
    , max(case when e.month_start_date is null then 1 else 0 end) as missing_eligibility
  from {{ ref('input_layer__pharmacy_claim') }} p
  left join {{ ref('data_quality__eligibility_dq_stage') }} e
    on p.person_id = e.person_id
    and coalesce(p.paid_date, p.dispensing_date) between e.month_start_date and e.month_end_date
  group by
      claim_id
)

, final as (
  select
      'multiple pharmacy_claim person_ids' as data_quality_check
    , sum(case when person_id_count > 1 then 1 else 0 end) as result_count
  from pharmacy

  union all

  select
      'missing pharmacy_claim person_id' as data_quality_check
    , sum(missing_person_id) as result_count
  from pharmacy

  union all

  select
      'orphaned pharmacy_claim claims' as data_quality_check
    , sum(missing_eligibility) as result_count
  from pharmacy
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
