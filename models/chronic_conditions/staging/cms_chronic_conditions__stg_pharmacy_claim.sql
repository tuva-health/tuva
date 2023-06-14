{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '{{ var('last_update')}}' as last_update
from {{ ref('pharmacy_claim') }}