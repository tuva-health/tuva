{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    procedure_family
  , procedure_name as procedure_name
  , status
from {{ ref('tuva_procedure_grouper') }}
where status != 'active'
