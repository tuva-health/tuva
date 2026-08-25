{{ config(
     severity = 'error',
     tags = ['contract', 'stable_id_contract']
   )
}}

{#
  Stable IDs are a cross-warehouse public contract. These fixtures pin both
  the unambiguous component encoding and the lowercase MD5 result so a future
  delimiter, null sentinel, escaping, or adapter change cannot silently
  rewrite every downstream identifier.
#}

with contract_cases as (
    select
          'clinical_domain_marker' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'clinical condition'", "'same'", "'id'"
          ]) }} as actual_id
        , 'd3fb3ac43e154b9d5ee12482893848b8' as expected_id

    union all

    select
          'claims_domain_marker' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "'same'", "'id'"
          ]) }} as actual_id
        , '23933cf98baa45c47b8a1096b0e23e25' as expected_id

    union all

    select
          'underscore_left_boundary' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'clinical condition'", "'a_b'", "'c'"
          ]) }} as actual_id
        , 'b4ace79d04a310770199a0a86a51a2e3' as expected_id

    union all

    select
          'underscore_right_boundary' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'clinical condition'", "'a'", "'b_c'"
          ]) }} as actual_id
        , 'ca0164fec970547072856a2e4f9c1c6a' as expected_id

    union all

    select
          'pipe_left_boundary' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "'a|b'", "'c'"
          ]) }} as actual_id
        , '35894b75c8e1d8f9feca119e442ff3ec' as expected_id

    union all

    select
          'pipe_right_boundary' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "'a'", "'b|c'"
          ]) }} as actual_id
        , '9ac6db57a8f353b4865775d7f566d0a2' as expected_id

    union all

    select
          'literal_percent_escape' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "'a%7Cb'", "'c'"
          ]) }} as actual_id
        , 'd727311ddc9af3d363a0cfc0af7ebcc2' as expected_id

    union all

    select
          'null_component' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "cast(null as " ~ dbt.type_string() ~ ")"
          ]) }} as actual_id
        , 'a061c1f253a8d5d3ac3f90c467ece1b7' as expected_id

    union all

    select
          'empty_component' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "''"
          ]) }} as actual_id
        , 'abb35b6992498bb1ca8f5124f42715e1' as expected_id

    union all

    select
          'literal_new_null_marker' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "'N'"
          ]) }} as actual_id
        , '2395c628c929ffad57b7e915ae36a828' as expected_id

    union all

    select
          'literal_legacy_null_marker' as case_name
        , {{ the_tuva_project.stable_id_hash([
              "'claims condition'", "'<NULL>'"
          ]) }} as actual_id
        , '7656c65f4a2cb8a7b279661e1db11d6c' as expected_id
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
