{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

-- Mapping status governs the reviewed assignment, not terminology lifecycle.
-- Historical/deprecated terminology codes therefore retain active mappings.

select
    code_system
  , code
  , status
from {{ ref('tuva_procedure_grouper_code_map') }}
where status != 'active'
