{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

with loinc as (

    select
        loinc_seed.loinc
      , loinc_seed.deprecated
    from {{ ref('terminology__loinc') }} as loinc_seed

)

, mappings as (

    select
        mapping_seed.loinc
      , mapping_seed.map_to
      , mapping_seed.final_map_to
    from {{ ref('terminology__loinc_deprecated_mapping') }} as mapping_seed

)

select
    mappings.loinc
  , mappings.map_to
  , mappings.final_map_to
  , 'missing_or_current_source' as issue
from mappings
left join loinc as source
  on mappings.loinc = source.loinc
where source.loinc is null
   or source.deprecated <> 1

union all

select
    mappings.loinc
  , mappings.map_to
  , mappings.final_map_to
  , 'missing_direct_target' as issue
from mappings
left join loinc as direct_target
  on mappings.map_to = direct_target.loinc
where direct_target.loinc is null

union all

select
    mappings.loinc
  , mappings.map_to
  , mappings.final_map_to
  , 'missing_final_target' as issue
from mappings
left join loinc as final_target
  on mappings.final_map_to = final_target.loinc
where final_target.loinc is null
