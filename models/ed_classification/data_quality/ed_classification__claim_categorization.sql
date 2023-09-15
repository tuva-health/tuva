/*
Highlights any lack of alignment between the ED records being used
for ED classification and the service category fields.
*/

{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
   service_category_1
   , service_category_2
   , count(*) as condition_count

from {{ ref('ed_classification__int_condition_with_claim') }}
group by service_category_1, service_category_2
order by count(*) desc
