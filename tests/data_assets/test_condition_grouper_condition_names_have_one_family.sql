{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

select
    condition
from {{ ref('tuva_condition_grouper') }}
group by
    condition
having count(distinct condition_family) != 1
