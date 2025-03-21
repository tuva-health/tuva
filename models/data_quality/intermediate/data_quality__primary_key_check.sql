{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with pharmacy as (
  select
      claim_id
    , claim_line_number
    , data_source
    , count(*) as result_count
  from {{ ref('input_layer__pharmacy_claim') }} p
  group by
      claim_id
    , claim_line_number
    , data_source
  having count(*) > 1
)

, medical as (
  select
      claim_id
    , claim_line_number
    , data_source
    , count(*) as result_count
  from {{ ref('input_layer__medical_claim') }} p
  group by
      claim_id
    , claim_line_number
    , data_source
  having count(*) > 1
)

, eligibility as (
  select
      person_id
    , enrollment_start_date
    , enrollment_end_date
    , {{ quote_column('plan') }}
    , data_source
    , count(*) as result_count
  from {{ ref('input_layer__eligibility') }} p
  group by
      person_id
    , enrollment_start_date
    , enrollment_end_date
    , {{ quote_column('plan') }}
    , data_source
  having count(*) > 1
)

, final as (
  select
      'pk errors pharmacy claim' as data_quality_check
    , coalesce(sum(result_count), 0) as result_count
  from pharmacy

  union all

  select
      'pk errors medical claim' as data_quality_check
    , coalesce(sum(result_count), 0) as result_count
  from medical

  union all

  select
      'pk errors eligibility' as data_quality_check
    , coalesce(sum(result_count), 0) as result_count
  from eligibility
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
