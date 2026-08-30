{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

with snomed_ct as (

    select
        cast(snomed_ct as {{ dbt.type_string() }}) as snomed_ct
      , upper(cast(is_active as {{ dbt.type_string() }})) as is_active
      , deprecated
    from {{ ref('terminology__snomed_ct') }}

)

, invalid_grain as (

    select snomed_ct
    from snomed_ct
    group by snomed_ct
    having snomed_ct is null
        or count(*) <> 1

)

, invalid_lifecycle as (

    select snomed_ct
    from snomed_ct
    where is_active is null
       or is_active not in ('Y', 'N')
       or (deprecated = 0 and is_active <> 'Y')

)

select
    snomed_ct
  , 'invalid_grain' as issue
from invalid_grain

union all

select
    snomed_ct
  , 'invalid_lifecycle' as issue
from invalid_lifecycle
