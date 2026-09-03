{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    procedure_name as procedure_name
from {{ ref('tuva_procedure_grouper') }}
group by
    procedure_name
having count(*) > 1
