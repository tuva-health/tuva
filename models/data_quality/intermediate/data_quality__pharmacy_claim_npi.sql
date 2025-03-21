{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with pharmacy_claim as (
    select
        m.claim_id
      , max(case when term.npi is null and m.prescribing_provider_npi is not null then 1 else 0 end) as invalid_prescribing_npi
      , max(case when m.prescribing_provider_npi is null then 1 else 0 end) as missing_prescribing_npi
      , max(case when term.entity_type_code = '2' then 1 else 0 end) as wrong_entity_type_prescribing_npi
      , max(case when term2.npi is null and m.dispensing_provider_npi is not null then 1 else 0 end) as invalid_dispensing_npi
      , max(case when m.dispensing_provider_npi is null then 1 else 0 end) as missing_dispensing_npi
      , max(case when term2.entity_type_code = '1' then 1 else 0 end) as wrong_entity_type_dispensing_npi
    from {{ ref('input_layer__pharmacy_claim') }} as m
    left join {{ ref('terminology__provider') }} as term
      on m.prescribing_provider_npi = term.npi
    left join {{ ref('terminology__provider') }} as term2
      on m.dispensing_provider_npi  = term2.npi
    group by
        m.claim_id
)

,final as (
select
    'invalid prescribing_npi' as data_quality_check
  , sum(invalid_prescribing_npi) as result_count
from pharmacy_claim

union all

select
    'missing prescribing_npi' as data_quality_check
  , sum(missing_prescribing_npi) as result_count
from pharmacy_claim

union all

select
    'wrong entity type prescribing_npi' as data_quality_check
  , sum(wrong_entity_type_prescribing_npi) as result_count
from pharmacy_claim

union all

select
    'invalid dispensing_npi' as data_quality_check
  , sum(invalid_dispensing_npi) as result_count
from pharmacy_claim

union all

select
    'missing dispensing_npi' as data_quality_check
  , sum(missing_dispensing_npi) as result_count
from pharmacy_claim

union all

select
    'wrong entity type dispensing_npi' as data_quality_check
  , sum(wrong_entity_type_dispensing_npi) as result_count
from pharmacy_claim
)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final