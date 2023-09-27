{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select 
    'service_category_1' as service_category_type
    , service_category_1 as service_category
    , count(distinct claim_id) as distinct_claim_count
from {{ ref('core__medical_claim') }}
group by service_category_1

union all

select 
    'service_category_2' as service_category_type
    , service_category_2 as service_category
    , count(distinct claim_id) as distinct_claim_count
from {{ ref('core__medical_claim') }}
group by service_category_2
