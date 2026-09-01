{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

select
    code_system
  , code
from {{ ref('tuva_condition_grouper_code_map') }}
where code_system != lower(trim(code_system))
   or code != case
        when lower(trim(code_system)) = 'icd-10-cm'
            then upper(replace(trim(code), '.', ''))
        else trim(code)
      end
