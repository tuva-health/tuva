{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  dq_problem
, count(distinct claim_id) as distinct_claims
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__dq_input_layer_tests') }}
group by 1