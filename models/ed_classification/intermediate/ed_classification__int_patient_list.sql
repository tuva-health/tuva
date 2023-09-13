{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
   p.*
   , c.claim_id
   , c.recorded_date
   , c.recorded_date_year
   , c.recorded_date_year_month
   , c.code_type
   , c.code
   , c.description
   , c.ccs_description_with_covid
   , ec.classification_order
   , c.classification
   , ec.classification_name
from {{ ref('ed_classification__int_condition_with_class') }} c
inner join {{ ref('ed_classification__stg_patient') }} p using(patient_id)
inner join {{ ref('ed_classification__categories') }} ec using(classification)
