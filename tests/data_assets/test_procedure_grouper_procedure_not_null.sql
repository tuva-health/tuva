{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    'tuva_procedure_grouper' as table_name
  , procedure_family
  , procedure_name
from {{ ref('tuva_procedure_grouper') }}
where procedure_name is null

union all

select
    'tuva_procedure_grouper_code_map' as table_name
  , procedure_family
  , procedure_name
from {{ ref('tuva_procedure_grouper_code_map') }}
where procedure_name is null
