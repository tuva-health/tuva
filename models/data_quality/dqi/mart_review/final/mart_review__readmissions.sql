{{ config(
     enabled = (var('enable_legacy_data_quality', false) | as_bool)
     and (var('claims_enabled', False) | as_bool)
   )
}}

select *
from {{ ref('readmissions__readmission_summary') }}
where index_admission_flag = 1
