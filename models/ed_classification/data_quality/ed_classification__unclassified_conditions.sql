/*
CCS Level breakdown of condition diagnoses and statistics
on the number of codes and condition rows that could not
be classified using the johnston algorithm
*/

{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
   ccs_description_with_covid
   , count(distinct(code)) as unique_codes_in_ccs_count
   , count(distinct(case when ed_classification_capture = 0 then code else null end)) as unique_unclassified_codes_in_ccs_count
   , count(code) as condition_row_count
   , (1 - avg(ed_classification_capture)) * 100 as condition_row_unclassified_percent
   , sum(case when ed_classification_capture = 0 then claim_paid_amount_sum else 0 end) as unclassified_claim_paid_amount_sum

from {{ ref('ed_classification__int_merge_condition') }}
group by ccs_description_with_covid
order by unclassified_claim_paid_amount_sum desc
