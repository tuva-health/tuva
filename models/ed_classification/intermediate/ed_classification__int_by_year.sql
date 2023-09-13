{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
   classification_order
   , classification_name
   , recorded_date_year
   , count(distinct(claim_id)) as claim_count
   , sum(claim_paid_amount_sum) as claim_paid_amount_sum
from {{ ref('ed_classification__summary') }}
group by classification_order, classification_name, recorded_date_year
order by recorded_date_year, classification_order
