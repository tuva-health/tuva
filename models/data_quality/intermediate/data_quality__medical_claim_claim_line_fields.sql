{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with medical_claim as (
  select
      m.claim_id
    , max(case when hcpc.hcpcs is null and m.hcpcs_code is not null then 1 else 0 end) as invalid_hcpcs_code
    , max(case when m.claim_type = 'professional' and pos.place_of_service_code is null and m.place_of_service_code is not null then 1 else 0 end) as invalid_place_of_service_code
    , max(case when m.claim_type = 'institutional' and rev.revenue_center_code is null and m.revenue_center_code is not null then 1 else 0 end) as invalid_revenue_center_code
    , max(case when m.hcpcs_code is null then 1 else 0 end) as missing_hcpcs_code
    , max(case when m.claim_type = 'professional' and m.place_of_service_code is null then 1 else 0 end) as missing_place_of_service_code
    , max(case when m.claim_type = 'institutional' and m.revenue_center_code is null then 1 else 0 end) as missing_revenue_center_code
  from {{ ref('input_layer__medical_claim') }} as m
  left join {{ ref('terminology__hcpcs_level_2') }} as hcpc
    on m.hcpcs_code = hcpc.hcpcs
  left join {{ ref('terminology__place_of_service') }} as pos
    on m.place_of_service_code = pos.place_of_service_code
  left join {{ ref('terminology__revenue_center') }} as rev
    on m.revenue_center_code = rev.revenue_center_code
  group by m.claim_id
)

, final as (
  select
      'invalid hcpcs_code' as data_quality_check
    , sum(invalid_hcpcs_code) as result_count
  from medical_claim
  
  union all

  select
      'invalid place_of_service_code' as data_quality_check
    , sum(invalid_place_of_service_code) as result_count
  from medical_claim

  union all

  select
      'invalid revenue_center_code' as data_quality_check
    , sum(invalid_revenue_center_code) as result_count
  from medical_claim

  union all

  select
      'missing hcpcs_code' as data_quality_check
    , sum(missing_hcpcs_code) as result_count
  from medical_claim

  union all

  select
      'missing place_of_service_code' as data_quality_check
    , sum(missing_place_of_service_code) as result_count
  from medical_claim

  union all

  select
      'missing revenue_center_code' as data_quality_check
    , sum(missing_revenue_center_code) as result_count
  from medical_claim
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
