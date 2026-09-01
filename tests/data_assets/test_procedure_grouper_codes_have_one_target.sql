{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

with normalized_code_targets as (

    select
        lower({{ the_tuva_project.trim('code_system') }}) as code_system
      , upper(replace({{ the_tuva_project.trim('code') }}, '.', '')) as code
      , procedure_family
      , {{ quote_column('procedure') }} as {{ quote_column('procedure') }}
    from {{ ref('tuva_procedure_grouper_code_map') }}

)

select
    code_system
  , code
from normalized_code_targets
group by
    code_system
  , code
having count(distinct procedure_family) != 1
    or count(distinct {{ quote_column('procedure') }}) != 1
