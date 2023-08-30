{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
  claim_id
, count(distinct service_category_2) as distinct_service_category_count
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__combined_institutional') }}
group by 1
having count(distinct service_category_2) > 1