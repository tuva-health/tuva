{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with pharmacy_claim as (
  select
      claim_id
    , max(case when term.ndc is null and m.ndc_code is not null then 1 else 0 end) as invalid_ndc
    , max(case when m.ndc_code is null then 1 else 0 end) as missing_ndc
  from {{ ref('input_layer__pharmacy_claim') }} m
  left join {{ ref('terminology__ndc') }} as term
    on m.ndc_code = term.ndc
  group by
      claim_id
)

, final as (
  select
      'missing ndc' as data_quality_check
    , sum(missing_ndc) as result_count
  from pharmacy_claim

  union all

  select
      'invalid ndc' as data_quality_check
    , sum(invalid_ndc) as result_count
  from pharmacy_claim
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
