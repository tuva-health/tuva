{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}
select *
from {{ ref('medical_claim') }}
