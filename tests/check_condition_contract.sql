{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            and (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     severity = 'error',
     tags = ['contract', 'condition_contract']
   )
}}

{#
  Integration-level wiring for the public Core condition contract. Focused
  unit tests pin the identifier algorithm and edge cases; this test proves the
  built Normalized, claims intermediate, and Core outputs retain the approved
  source and semantic values.
#}

with normalized_clinical as (
    select
          condition_id
        , source_condition_id
        , data_source
    from {{ ref('normalized__condition') }}
)

, claims_conditions as (
    select
          condition_id
        , source_condition_id
        , claim_id
        , status
        , condition_type
        , data_source
    from {{ ref('int_condition_from_claims') }}
)

, core_conditions as (
    select
          condition_id
        , source_condition_id
        , claim_id
        , status
        , condition_type
        , data_source
    from {{ ref('core__condition') }}
)

, contract_failures as (
    select
          'normalized_source_condition_id_null' as failure_type
        , condition_id
        , data_source
    from normalized_clinical
    where source_condition_id is null

    union all

    select
          'claims_intermediate_source_condition_id_populated' as failure_type
        , condition_id
        , data_source
    from claims_conditions
    where source_condition_id is not null

    union all

    select
          'claims_intermediate_status_populated' as failure_type
        , condition_id
        , data_source
    from claims_conditions
    where status is not null

    union all

    select
          'claims_intermediate_condition_type_invalid' as failure_type
        , condition_id
        , data_source
    from claims_conditions
    where condition_type is null
       or condition_type <> 'billing_diagnosis'

    union all

    select
          'core_claims_source_condition_id_populated' as failure_type
        , condition_id
        , data_source
    from core_conditions
    where claim_id is not null
      and source_condition_id is not null

    union all

    select
          'core_claims_status_populated' as failure_type
        , condition_id
        , data_source
    from core_conditions
    where claim_id is not null
      and status is not null

    union all

    select
          'core_claims_condition_type_invalid' as failure_type
        , condition_id
        , data_source
    from core_conditions
    where claim_id is not null
      and (condition_type is null or condition_type <> 'billing_diagnosis')

    union all

    select
          'core_clinical_source_condition_id_changed' as failure_type
        , normalized_clinical.condition_id
        , normalized_clinical.data_source
    from normalized_clinical
    left join core_conditions
        on normalized_clinical.condition_id = core_conditions.condition_id
        and normalized_clinical.data_source = core_conditions.data_source
    where core_conditions.condition_id is null
       or core_conditions.source_condition_id is null
       or normalized_clinical.source_condition_id <> core_conditions.source_condition_id
)

select
      failure_type
    , condition_id
    , data_source
from contract_failures
