{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
  claim_id
, service_type
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_inpatient_institutional') }} as a
