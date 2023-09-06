{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim') }}