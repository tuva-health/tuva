{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

select
    code_map.code_system
  , code_map.code
  , code_map.condition_family
  , code_map.condition
from {{ ref('tuva_condition_grouper_code_map') }} as code_map
left join {{ ref('tuva_condition_grouper') }} as condition_grouper
  on code_map.condition_family = condition_grouper.condition_family
  and code_map.condition = condition_grouper.condition
where condition_grouper.condition_family is null

