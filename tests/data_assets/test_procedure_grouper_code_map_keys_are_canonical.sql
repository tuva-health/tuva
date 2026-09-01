{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    code_system
  , code
from {{ ref('tuva_procedure_grouper_code_map') }}
where code_system != 'icd-10-pcs'
   or code != upper(replace({{ the_tuva_project.trim('code') }}, '.', ''))
   or length(code) != 7
