{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    med.claim_id
  , 'outpatient' as service_category_1
  , 'dialysis' as service_category_2
  , 'dialysis' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as outpatient
  on med.claim_id = outpatient.claim_id
where
  substring(med.bill_type_code, 1, 2) in ('72')
  or med.primary_taxonomy_code in (
      '2472R0900X'
    , '163WD1100X'
    , '163WH0500X'
    , '261QE0700X'
  )
  or med.ccs_category in ('91', '58', '57')
  or substring(med.revenue_center_code, 1, 3) in ('082', '083', '084', '085', '088')
