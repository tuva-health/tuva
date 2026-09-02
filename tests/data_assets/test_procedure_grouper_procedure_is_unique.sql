{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    {{ quote_column('procedure') }} as {{ quote_column('procedure') }}
from {{ ref('tuva_procedure_grouper') }}
group by
    {{ quote_column('procedure') }}
having count(*) > 1
