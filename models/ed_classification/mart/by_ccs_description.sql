{{ config(enabled=var('ed_classification_enabled',var('tuva_packages_enabled',True))) }}

with summary as (
   select
      classification_order
      , classification_name
      , condition_date_year
      , ccs_description_with_covid
      , count(distinct(claim_id)) as claim_count
      , sum(claim_paid_amount_sum) as claim_paid_amount_sum

   from {{ ref('ed_summary') }}
   group by classification_order, classification_name, condition_date_year, ccs_description_with_covid
)

select
    summary.*
    , 100 * ratio_to_report(claim_count)
      over(partition by classification_name) as percent_claim_count_of_classification
    , 100 * ratio_to_report(claim_paid_amount_sum)
      over(partition by classification_name) as "percent_claim_paid_amount_sum_of_classification"

from summary
order by "percent_claim_paid_amount_sum_of_classification" desc
