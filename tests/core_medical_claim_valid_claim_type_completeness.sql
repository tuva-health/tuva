{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false),
     severity = 'error',
     tags = ['core', 'medical_claim_contract']
   )
}}

{#
  Claim type routes a valid claim line through claims preprocessing; it must
  not decide whether that line reaches Core. Reconcile all contract-valid
  normalized lines to Core and lock the conservative semantics used when the
  billing form is undetermined.
#}

with expected_claim_lines as (
    select
          medical_claim_id
        , claim_id
        , claim_line_number
        , claim_type
        , data_source
        , count(*) as expected_count
    from {{ ref('normalized__medical_claim') }}
    where claim_type in ('professional', 'institutional', 'undetermined')
      and claim_id is not null
      and claim_line_number is not null
      and person_id is not null
      and data_source is not null
    group by
          medical_claim_id
        , claim_id
        , claim_line_number
        , claim_type
        , data_source
)

, actual_claim_lines as (
    select
          medical_claim_id
        , claim_id
        , claim_line_number
        , claim_type
        , data_source
        , count(*) as actual_count
    from {{ ref('core__medical_claim') }}
    group by
          medical_claim_id
        , claim_id
        , claim_line_number
        , claim_type
        , data_source
)

, missing_or_duplicate_claim_lines as (
    select
          'missing_or_duplicate_core_claim_line' as failure_type
        , expected.medical_claim_id
        , expected.claim_id
        , expected.claim_line_number
        , expected.claim_type
        , expected.data_source
        , expected.expected_count
        , coalesce(actual.actual_count, 0) as actual_count
    from expected_claim_lines as expected
    left join actual_claim_lines as actual
        on expected.medical_claim_id = actual.medical_claim_id
        and expected.claim_id = actual.claim_id
        and expected.claim_line_number = actual.claim_line_number
        and expected.claim_type = actual.claim_type
        and expected.data_source = actual.data_source
    where expected.expected_count <> coalesce(actual.actual_count, 0)
)

, unexpected_claim_lines as (
    select
          'unexpected_core_claim_line' as failure_type
        , actual.medical_claim_id
        , actual.claim_id
        , actual.claim_line_number
        , actual.claim_type
        , actual.data_source
        , 0 as expected_count
        , actual.actual_count
    from actual_claim_lines as actual
    left join expected_claim_lines as expected
        on actual.medical_claim_id = expected.medical_claim_id
        and actual.claim_id = expected.claim_id
        and actual.claim_line_number = expected.claim_line_number
        and actual.claim_type = expected.claim_type
        and actual.data_source = expected.data_source
    where expected.medical_claim_id is null
)

, invalid_undetermined_semantics as (
    select
          'invalid_undetermined_core_semantics' as failure_type
        , medical_claim_id
        , claim_id
        , claim_line_number
        , claim_type
        , data_source
        , 1 as expected_count
        , 1 as actual_count
    from {{ ref('core__medical_claim') }}
    where claim_type = 'undetermined'
      and (
           coalesce(service_category_1, '') <> 'other'
        or coalesce(service_category_2, '') <> 'other'
        or coalesce(service_category_3, '') <> 'other'
        or coalesce(encounter_type, '') <> 'orphaned claim'
        or coalesce(encounter_group, '') <> 'other'
      )
)

select *
from missing_or_duplicate_claim_lines

union all

select *
from unexpected_claim_lines

union all

select *
from invalid_undetermined_semantics
