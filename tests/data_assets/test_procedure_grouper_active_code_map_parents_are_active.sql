{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    code_map.code_system
  , code_map.code
  , code_map.procedure_family
  , code_map.{{ quote_column('procedure') }} as {{ quote_column('procedure') }}
from {{ ref('tuva_procedure_grouper_code_map') }} as code_map
left join {{ ref('tuva_procedure_grouper') }} as procedure_grouper
  on code_map.procedure_family = procedure_grouper.procedure_family
  and code_map.{{ quote_column('procedure') }} = procedure_grouper.{{ quote_column('procedure') }}
  and procedure_grouper.status = 'active'
where code_map.status = 'active'
  and procedure_grouper.procedure_family is null
