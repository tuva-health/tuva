{{ config(
     enabled = var('acute_inpatient_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  claim_id
, claim_type
, service_category_2
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__service_category_grouper')}}