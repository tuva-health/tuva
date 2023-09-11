/*
Highlights any lack of alignment between the ED records being used
for ED classification and the service category fields.
*/

{{ config(enabled=var('ed_classification_enabled',var('tuva_packages_enabled',True))) }}

select
   service_category_1
   , service_category_2
   , count(*) as condition_count

from {{ ref('ed_classified_condition_with_claim') }}
group by service_category_1, service_category_2
order by count(*) desc
