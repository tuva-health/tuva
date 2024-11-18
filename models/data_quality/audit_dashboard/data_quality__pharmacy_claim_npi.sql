{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
   )
}}

with pharmacy_claim as (
    select
        m.claim_id
      , max(case when term.entity_type_code = '2' then 1 else 0 end) as wrong_entity_type_prescribing_npi
      , max(case when term2.entity_type_code = '1' then 1 else 0 end) as wrong_entity_type_dispensing_npi
    from {{ ref('pharmacy_claim') }} as m
    left join {{ ref('terminology__provider') }} as term
      on m.prescribing_provider_npi = term.npi
    left join {{ ref('terminology__provider') }} as term2
      on m.dispensing_provider_npi  = term2.npi
    group by
        m.claim_id
)

,final as (
select
    'wrong entity type prescribing_npi' as data_quality_check
  , sum(wrong_entity_type_prescribing_npi) as result_count
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