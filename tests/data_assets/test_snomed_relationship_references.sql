{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

with snomed_ct as (

    select cast(snomed_seed.snomed_ct as {{ dbt.type_string() }}) as snomed_ct
    from {{ ref('terminology__snomed_ct') }} as snomed_seed

)

, icd_10_cm as (

    select cast(icd_10_cm_seed.icd_10_cm as {{ dbt.type_string() }}) as icd_10_cm
    from {{ ref('terminology__icd_10_cm') }} as icd_10_cm_seed

)

, transitive_closures as (

    select
        cast(closure_seed.parent_snomed_code as {{ dbt.type_string() }}) as parent_snomed_code
      , cast(closure_seed.child_snomed_code as {{ dbt.type_string() }}) as child_snomed_code
    from {{ ref('terminology__snomed_ct_transitive_closures') }} as closure_seed

)

, snomed_icd_10_map as (

    select
        cast(map_seed.referenced_component_id as {{ dbt.type_string() }}) as referenced_component_id
      , cast(map_seed.map_target as {{ dbt.type_string() }}) as map_target
    from {{ ref('terminology__snomed_icd_10_map') }} as map_seed

)

select
    transitive_closures.parent_snomed_code as code
  , 'missing_closure_parent' as issue
from transitive_closures
left join snomed_ct
  on transitive_closures.parent_snomed_code = snomed_ct.snomed_ct
where snomed_ct.snomed_ct is null

union all

select
    transitive_closures.child_snomed_code as code
  , 'missing_closure_child' as issue
from transitive_closures
left join snomed_ct
  on transitive_closures.child_snomed_code = snomed_ct.snomed_ct
where snomed_ct.snomed_ct is null

union all

select
    snomed_icd_10_map.referenced_component_id as code
  , 'missing_map_source' as issue
from snomed_icd_10_map
left join snomed_ct
  on snomed_icd_10_map.referenced_component_id = snomed_ct.snomed_ct
where snomed_ct.snomed_ct is null

union all

select
    snomed_icd_10_map.map_target as code
  , 'missing_exact_icd_10_cm_target' as issue
from snomed_icd_10_map
left join icd_10_cm
  on snomed_icd_10_map.map_target = icd_10_cm.icd_10_cm
where snomed_icd_10_map.map_target is not null
  and snomed_icd_10_map.map_target <> ''
  and snomed_icd_10_map.map_target not like '%?%'
  and icd_10_cm.icd_10_cm is null
