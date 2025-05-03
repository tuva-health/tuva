{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with diabetes_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
              'diabetes'
        )

)

, conditions as (

    select
          person_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from {{ ref('quality_measures__stg_core__condition') }}

)

, diabetes_conditions as (

    select
          conditions.person_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join diabetes_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = diabetes_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = diabetes_codes.code

)

, patients_with_diabetes as (

    select
        distinct
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from diabetes_conditions
    inner join {{ ref('quality_measures__int_cqm438__performance_period') }} as pp
    on evidence_date <= pp.performance_period_end

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
    from patients_with_diabetes

)

select
      person_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
