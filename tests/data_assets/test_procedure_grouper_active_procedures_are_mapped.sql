{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    procedure_grouper.procedure_family
  , procedure_grouper.procedure_name as procedure_name
from {{ ref('tuva_procedure_grouper') }} as procedure_grouper
left join {{ ref('tuva_procedure_grouper_code_map') }} as code_map
  on procedure_grouper.procedure_family = code_map.procedure_family
  and procedure_grouper.procedure_name = code_map.procedure_name
  and code_map.status = 'active'
where procedure_grouper.status = 'active'
group by
    procedure_grouper.procedure_family
  , procedure_grouper.procedure_name
having count(code_map.code) = 0
