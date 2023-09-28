{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select 
    claim_type
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
from {{ ref('core__medical_claim') }}
group by claim_type

union all

select 
    'pharmacy'
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , null as total_charge_amount
from {{ ref('core__pharmacy_claim') }}