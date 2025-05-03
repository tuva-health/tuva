{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with multiple_sources as (
    select distinct
        m.claim_id
      , 'outpatient rehabilitation' as service_category_2
      , 'outpatient rehabilitation' as service_category_3
      , '{{ this.name }}' as source_model_name
      , '{{ var('tuva_last_run') }}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} as m
    inner join {{ ref('service_category__stg_outpatient_institutional') }} as i
      on m.claim_id = i.claim_id
    where
      m.primary_taxonomy_code in (
          '283X00000X'
        , '273Y00000X'
        , '261QR0400X'
        , '315D00000X'
        , '261QR0401X'
        , '208100000X'
        , '225400000X'
        , '324500000X'
        , '2278P1005X'
        , '261QR0405X'
        , '2081S0010X'
        , '261QR0404X'
      )
)

select distinct
    claim_id
  , 'outpatient' as service_category_1
  , service_category_2
  , service_category_3
  , source_model_name
  , tuva_last_run
from multiple_sources
