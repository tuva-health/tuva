{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}
select *
from {{ ref('location') }}
