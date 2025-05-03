{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    a.claim_id
  , 'inpatient' as service_category_1
  , 'inpatient substance use' as service_category_2
  , 'inpatient substance use' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as s
inner join {{ ref('service_category__stg_inpatient_institutional') }} as a
  on s.claim_id = a.claim_id
where
  s.primary_taxonomy_code in (
      '324500000X'
    , '261QR0405X'
    , '101YA0400X'
  )
  or s.default_ccsr_category_description_ip in (
      'MBD026'
    , 'SYM008'
    , 'MBD025'
    , 'SYM009'
    , 'MBD034'
  )
