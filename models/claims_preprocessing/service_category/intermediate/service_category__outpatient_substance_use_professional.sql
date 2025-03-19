{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    med.claim_id
  , med.claim_line_number
  , med.claim_line_id
  , 'outpatient' as service_category_1
  , 'outpatient substance use' as service_category_2
  , 'outpatient substance use' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_professional') }} as prof
  on med.claim_id = prof.claim_id
  and med.claim_line_number = prof.claim_line_number
where 
  (med.default_ccsr_category_description_op in (
      'MBD026'
    , 'SYM008'
    , 'MBD025'
    , 'SYM009'
    , 'MBD034'
  )
  and med.place_of_service_code <> '11')
  or med.place_of_service_code in ('57','58')
