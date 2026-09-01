{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    procedure_family
  , {{ quote_column('procedure') }} as {{ quote_column('procedure') }}
  , status
from {{ ref('tuva_procedure_grouper') }}
where status != 'active'
