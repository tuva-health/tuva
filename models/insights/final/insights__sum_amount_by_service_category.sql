{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    'service_category_1' as service_category_type
    , service_category_1
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
from {{ ref('core__medical_claim') }}
group by service_category_1

union all

select
    'service_category_2' as service_category_type
    , service_category_2
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
from {{ ref('core__medical_claim') }}
group by service_category_2