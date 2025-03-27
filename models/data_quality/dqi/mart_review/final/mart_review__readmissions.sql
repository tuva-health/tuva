{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select *
from {{ ref('readmissions__readmission_summary') }}
where index_admission_flag = 1
