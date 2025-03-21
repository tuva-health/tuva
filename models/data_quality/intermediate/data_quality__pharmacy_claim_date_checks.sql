{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with pharmacy_claim as (
  select
      claim_id
    , max(case when paid_date is null then 1 else 0 end) as missing_paid_date
    , max(case when dispensing_date is null then 1 else 0 end) as missing_dispensing_date
  from {{ ref('input_layer__pharmacy_claim') }}
  group by
      claim_id
)

, final as (
  select
      'missing pharmacy_claim paid_date' as data_quality_check
    , sum(missing_paid_date) as result_count
  from pharmacy_claim

  union all

  select
      'missing dispensing_date' as data_quality_check
    , sum(missing_dispensing_date) as result_count
  from pharmacy_claim
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
