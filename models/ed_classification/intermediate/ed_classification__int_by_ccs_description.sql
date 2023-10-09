{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with summary as (
   select
      classification_order
      , classification_name
      , recorded_date_year
      , ccs_description_with_covid
      , count(distinct(claim_id)) as claim_count
      , sum(claim_paid_amount_sum) as claim_paid_amount_sum
   from {{ ref('ed_classification__summary') }}
   group by classification_order, classification_name, recorded_date_year, ccs_description_with_covid
)
, totals_by_classification as (
    select
        classification_name
        , count(distinct(claim_id)) as total_claim_count
        , sum(claim_paid_amount_sum) as total_claim_paid_amount
    from {{ ref('ed_classification__summary') }}
    group by
        classification_name
)

select
    summary.*
    ,claim_count/total_claim_count * 100 as percent_claim_count_of_classification
    , claim_paid_amount_sum/total_claim_paid_amount * 100 as percent_claim_paid_amount_sum_of_classification
from summary
inner join totals_by_classification class
    on summary.classification_name = class.classification_name
order by percent_claim_paid_amount_sum_of_classification desc
