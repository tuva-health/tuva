{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  claim_id
, count(distinct service_category_2) as distinct_service_category_count
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__combined_institutional') }}
group by 1
having count(distinct service_category_2) > 1