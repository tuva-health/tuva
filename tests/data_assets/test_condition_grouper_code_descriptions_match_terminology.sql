{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

with terminology_descriptions as (

    select
        'icd-10-cm' as code_system
      , icd_10_cm as code
      , coalesce(nullif(long_description, ''), short_description) as code_description
    from {{ ref('terminology__icd_10_cm') }}

    union all

    select
        'snomed-ct' as code_system
      , snomed_ct as code
      , description as code_description
    from {{ ref('terminology__snomed_ct') }}

)

select
    code_map.code_system
  , code_map.code
  , code_map.code_description
  , terminology.code_description as terminology_description
from {{ ref('tuva_condition_grouper_code_map') }} as code_map
inner join terminology_descriptions as terminology
  on code_map.code_system = terminology.code_system
  and code_map.code = terminology.code
where code_map.code_description != terminology.code_description
