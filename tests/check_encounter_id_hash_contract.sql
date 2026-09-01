{{ config(
     severity = 'error',
     tags = ['contract', 'stable_id_contract']
   )
}}

{#
  Encounter IDs share the collision-safe stable-ID serialization. These cases
  pin delimiter boundaries and the former literal <NULL> sentinel so a later
  macro change cannot silently reintroduce either collision class.
#}

with contract_cases as (
    select
          'pipe_left_boundary' as case_name
        , {{ the_tuva_project.encounter_id_hash([
              "'outpatient visit'", "'a|b'", "'c'"
          ]) }} as actual_id
        , '400a99d1fda4fae42d75ea2ea10484a0' as expected_id

    union all

    select
          'pipe_right_boundary' as case_name
        , {{ the_tuva_project.encounter_id_hash([
              "'outpatient visit'", "'a'", "'b|c'"
          ]) }} as actual_id
        , '08c26e2f270adb2fd490118eee57c0ec' as expected_id

    union all

    select
          'null_component' as case_name
        , {{ the_tuva_project.encounter_id_hash([
              "'outpatient visit'",
              "cast(null as " ~ dbt.type_string() ~ ")",
              "'c'"
          ]) }} as actual_id
        , 'a2ca8a74fabface17f33e83d7ea9f914' as expected_id

    union all

    select
          'literal_legacy_null_marker' as case_name
        , {{ the_tuva_project.encounter_id_hash([
              "'outpatient visit'", "'<NULL>'", "'c'"
          ]) }} as actual_id
        , '2f9676fb1f59f056a7e027e9ad9ee389' as expected_id
)

, duplicate_ids as (
    select actual_id
    from contract_cases
    group by actual_id
    having count(*) > 1
)

select
      case_name
    , actual_id
    , expected_id
from contract_cases
where actual_id <> expected_id

union all

select
      'duplicate_contract_id' as case_name
    , actual_id
    , cast(null as {{ dbt.type_string() }}) as expected_id
from duplicate_ids
