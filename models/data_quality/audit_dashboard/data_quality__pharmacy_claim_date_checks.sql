{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with pharmacy_claim as (
  select
      claim_id
    , max(case when dispensing_date > current_date() then 1 else 0 end) as future_dispensing
  from {{ ref('pharmacy_claim') }}
  group by
      claim_id
)
select
    'dispensing_date in future' as data_quality_check
    , count(distinct claim_id) as result_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from pharmacy_claim
where future_dispensing > 1
