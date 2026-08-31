{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

with snomed_ct as (

    select cast(snomed_ct as {{ dbt.type_string() }}) as snomed_ct
    from {{ ref('terminology__snomed_ct') }}

)

, reflexive_edges as (

    select
        cast(parent_snomed_code as {{ dbt.type_string() }}) as snomed_ct
      , count(*) as row_count
    from {{ ref('terminology__snomed_ct_transitive_closures') }}
    where parent_snomed_code = child_snomed_code
    group by cast(parent_snomed_code as {{ dbt.type_string() }})

)

select
    snomed_ct.snomed_ct
  , coalesce(reflexive_edges.row_count, 0) as row_count
from snomed_ct
left join reflexive_edges
  on snomed_ct.snomed_ct = reflexive_edges.snomed_ct
where coalesce(reflexive_edges.row_count, 0) <> 1
