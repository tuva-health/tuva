{{ config(
     enabled = var('acute_inpatient_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  claim_id
, claim_type
, service_category_2
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__service_category_grouper')}}