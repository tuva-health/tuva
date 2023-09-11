{{ config(enabled=var('ed_classification_enabled',var('tuva_packages_enabled',True))) }}


select
   p.*
   , c.claim_id
   , c.condition_date
   , c.condition_date_year
   , c.condition_date_year_month
   , c.code_type
   , c.code
   , c.description
   , c.ccs_description_with_covid
   , ec.classification_order
   , c.classification
   , ec.classification_name
from {{ ref('ed_classification__int_condition_with_class') }} c
inner join {{ ref('core__patient') }} p using(patient_id)
inner join {{ ref('ed_classification__categories') }} ec using(classification)
