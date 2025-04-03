{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with multiple_sources as (
    select distinct 
        m.claim_id
      , 'outpatient hospital or clinic' as service_category_2
      , 'outpatient hospital or clinic' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} as m
    inner join {{ ref('service_category__stg_outpatient_institutional') }} as o
      on m.claim_id = o.claim_id
    where 
      substring(m.bill_type_code, 1, 2) in (
        '13',  -- Hospital Outpatient
        '43',  -- Religious Nonmedical Hospital Outpatient
        '44',  -- Religious Nonmedical Hospital Other (Part B)
        '71',  -- Clinic Rural Health Center (RHC)
        '72',  -- Clinic Hospital-based or Independent Renal Dialysis Center
        '73',  -- Clinic Federally Qualified Health Center (FQHC)
        '74',  -- Clinic Other Rehabilitation Facility (ORF)
        '75',  -- Clinic Comprehensive Outpatient Rehabilitation Facility (CORF)
        '76',  -- Clinic Community Mental Health Center (CMHC)
        '77',  -- Clinic Free-standing Provider-based FQHC
        '79'   -- Clinic Other
      )

    union all

    select distinct
        m.claim_id
      , 'outpatient hospital or clinic' as service_category_2
      , 'outpatient hospital or clinic' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} as m
    inner join {{ ref('service_category__stg_outpatient_institutional') }} as o
      on m.claim_id = o.claim_id
    where 
      m.ccs_category = '227' -- Consultation, evaluation, and preventative care
)

select distinct
    claim_id
  , 'outpatient' as service_category_1
  , service_category_2
  , service_category_3
  , source_model_name
  , tuva_last_run
from multiple_sources
