{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

with normalized_code_keys as (

    select
        lower(trim(code_system)) as code_system
      , case
            when lower(trim(code_system)) = 'icd-10-cm'
                then upper(replace(trim(code), '.', ''))
            else trim(code)
        end as code
    from {{ ref('tuva_condition_grouper_code_map') }}

)

select
    code_system
  , code
from normalized_code_keys
group by
    code_system
  , code
having count(*) > 1
