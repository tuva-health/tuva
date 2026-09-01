{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

select
    condition_grouper.condition_family
  , condition_grouper.condition
from {{ ref('tuva_condition_grouper') }} as condition_grouper
left join {{ ref('tuva_condition_grouper_code_map') }} as code_map
  on condition_grouper.condition_family = code_map.condition_family
  and condition_grouper.condition = code_map.condition
  and code_map.status = 'active'
where condition_grouper.status = 'active'
group by
    condition_grouper.condition_family
  , condition_grouper.condition
having count(code_map.code) = 0
