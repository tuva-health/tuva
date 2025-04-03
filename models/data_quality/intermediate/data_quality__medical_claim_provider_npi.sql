{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with medical_claim as (
    select
        m.claim_id
      , max(case when term.npi is null and m.rendering_npi is not null then 1 else 0 end) as invalid_rendering_npi
      , max(case when term2.npi is null and m.billing_npi is not null then 1 else 0 end) as invalid_billing_npi
      , max(case when term3.npi is null and m.facility_npi is not null then 1 else 0 end) as invalid_facility_npi

      , max(case when m.rendering_npi is null then 1 else 0 end) as missing_rendering_npi
      , max(case when m.billing_npi is null then 1 else 0 end) as missing_billing_npi
      , max(case when m.facility_npi is null then 1 else 0 end) as missing_facility_npi

      , count(distinct m.rendering_npi) as rendering_npi_count
      , count(distinct m.billing_npi) as billing_npi_count
      , count(distinct m.facility_npi) as facility_npi_count

      , max(case when term.entity_type_code = '2' then 1 else 0 end) as wrong_entity_type_rendering_npi
      , max(case when term3.entity_type_code = '1' then 1 else 0 end) as wrong_entity_type_facility_npi

    from {{ ref('input_layer__medical_claim') }} as m
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
    'invalid rendering_npi' as data_quality_check
  , sum(invalid_rendering_npi) as result_count
from medical_claim

union all

select
    'invalid billing_npi' as data_quality_check
  , sum(invalid_billing_npi) as result_count
from medical_claim

union all

select
    'invalid facility_npi' as data_quality_check
  , sum(invalid_facility_npi) as result_count
from medical_claim

union all

select
    'missing rendering_npi' as data_quality_check
  , sum(missing_rendering_npi) as result_count
from medical_claim

union all

select
    'missing billing_npi' as data_quality_check
  , sum(missing_billing_npi) as result_count
from medical_claim

union all

select
    'missing facility_npi' as data_quality_check
  , sum(missing_facility_npi) as result_count
from medical_claim

union all

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