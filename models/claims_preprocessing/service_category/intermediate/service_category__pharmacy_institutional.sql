{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with multiple_sources as (
    select distinct
        med.claim_id
      , med.claim_line_number
      , 'outpatient' as service_category_1
      , 'pharmacy' as service_category_2
      , 'pharmacy' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} as med
    inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
      on med.claim_id = outpatient.claim_id
    where
      (substring(med.revenue_center_code, 1, 3) in ('025', '026', '063', '089') -- pharmacy and iv therapy
      or med.revenue_center_code = '0547'
      or med.ccs_category = '240') -- medications

    union all

    select distinct
        med.claim_id
      , med.claim_line_number
      , 'inpatient' as service_category_1
      , 'pharmacy' as service_category_2
      , 'pharmacy' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} as med
    inner join {{ ref('service_category__stg_inpatient_institutional') }} as outpatient
      on med.claim_id = outpatient.claim_id
    where
      (substring(med.revenue_center_code, 1, 3) in ('025', '026', '063', '089') -- pharmacy and iv therapy
      or med.revenue_center_code = '0547')
)

select
    claim_id
  , claim_line_number
  , service_category_1
  , service_category_2
  , service_category_3
  , source_model_name
  , tuva_last_run
from multiple_sources
