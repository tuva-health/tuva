{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
   )
}}

with medical_claim as (
    select
        m.claim_id

      , count(distinct m.rendering_npi) as rendering_npi_count
      , count(distinct m.billing_npi) as billing_npi_count
      , count(distinct m.facility_npi) as facility_npi_count

      , max(case when term.entity_type_code = '2' then 1 else 0 end) as wrong_entity_type_rendering_npi
      , max(case when term3.entity_type_code = '1' then 1 else 0 end) as wrong_entity_type_facility_npi

    from {{ ref('medical_claim') }} as m
    left join {{ ref('terminology__provider') }} as term
      on m.rendering_npi = term.npi
    left join {{ ref('terminology__provider') }} as term2
      on m.billing_npi  = term2.npi
    left join {{ ref('terminology__provider') }} as term3
      on m.facility_npi = term3.npi
    group by
        m.claim_id
)

,final as (
select
    'wrong entity type rendering_npi' as data_quality_check
  , sum(wrong_entity_type_rendering_npi) as result_count
from medical_claim

union all

select
    'wrong entity type facility_npi' as data_quality_check
  , sum(wrong_entity_type_facility_npi) as result_count
from medical_claim

union all

select
    'multiple rendering_npi values' as data_quality_check
  , count(distinct claim_id) as result_count
from medical_claim
where rendering_npi_count > 1

union all

select
    'multiple billing_npi values' as data_quality_check
  , count(distinct claim_id) as result_count
from medical_claim
where billing_npi_count > 1

union all

select
    'multiple facility_npi values' as data_quality_check
  , count(distinct claim_id) as result_count
from medical_claim
where facility_npi_count > 1
)

select *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final