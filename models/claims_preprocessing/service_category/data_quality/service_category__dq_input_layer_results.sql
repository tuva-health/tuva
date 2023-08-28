{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  dq_problem
, count(distinct claim_id) as distinct_claims
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__dq_input_layer_tests') }}
group by 1