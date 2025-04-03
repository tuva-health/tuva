{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with pharmacy_claim as (
  select
      claim_id
    , max(case 
        when quantity is null then 1
        when quantity = 0 then 1
        else 0
      end) as missing_quantity
    , max(case 
        when days_supply is null then 1
        when days_supply = 0 then 1
        else 0
      end) as missing_days_supply
    , max(case 
        when refills is null then 1
        else 0
      end) as missing_refills
  from {{ ref('input_layer__pharmacy_claim') }} m
  group by
      claim_id
)

, final as (
  select
      'missing quantity' as data_quality_check
    , sum(missing_quantity) as result_count
  from pharmacy_claim

  union all

  select
      'missing days supply' as data_quality_check
    , sum(missing_days_supply) as result_count
  from pharmacy_claim

  union all

  select
      'missing refills' as data_quality_check
    , sum(missing_refills) as result_count
  from pharmacy_claim
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
