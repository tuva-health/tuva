{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

-- Nonbillable ICD-10-CM headings occur in real-world source data even though
-- they are not valid billable claim diagnoses. Every retained heading must
-- therefore have one active analytic assignment.

select
    terminology.icd_10_cm as code
from {{ ref('terminology__icd_10_cm') }} as terminology
left join {{ ref('tuva_condition_grouper_code_map') }} as code_map
    on code_map.code_system = 'icd-10-cm'
    and code_map.code = upper(replace(
        {{ the_tuva_project.trim('terminology.icd_10_cm') }}, '.', ''
    ))
    and code_map.status = 'active'
where terminology.billable_code_flag = 0
  and code_map.code is null
