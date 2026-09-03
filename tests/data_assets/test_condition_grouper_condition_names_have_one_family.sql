{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

select
    condition_name
from {{ ref('tuva_condition_grouper') }}
group by
    condition_name
having count(distinct condition_family) != 1
