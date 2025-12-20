{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    med.claim_id
  , med.data_source
  , 'outpatient' as service_category_1
  , 'outpatient hospice' as service_category_2
  , 'outpatient hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
  on med.claim_id = outpatient.claim_id
  and med.data_source = outpatient.data_source
where
  substring(med.bill_type_code, 1, 2) in ('81')
  or (
    med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5009')
    and not exists (
      select 1
      from {{ ref('service_category__home_health_institutional') }} as hhi
      where med.claim_id = hhi.claim_id
      and med.data_source = hhi.data_source
    )
  )
  or med.revenue_center_code in ('0650', '0651', '0652', '0657', '0659')
