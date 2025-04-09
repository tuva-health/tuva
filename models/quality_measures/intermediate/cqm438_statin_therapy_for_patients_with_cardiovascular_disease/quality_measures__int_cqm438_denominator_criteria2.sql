{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with cholesterol_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
              'ldl cholesterol'
            , 'familial hypercholesterolemia'
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

, cholesterol_conditions as (

    select
          conditions.person_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join cholesterol_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = cholesterol_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = cholesterol_codes.code

)

, procedures as (

    select
          person_id
        , procedure_date
        , coalesce(
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, cholesterol_procedures as (

    select
          procedures.person_id
        , procedures.procedure_date as evidence_date
    from procedures
         inner join cholesterol_codes
             on procedures.code = cholesterol_codes.code
             and procedures.code_type = cholesterol_codes.code_system

)

, labs as (

    select
          person_id
        , result
        , result_date
        , collection_date
        , source_code_type
        , source_code
        , normalized_code_type
        , normalized_code
    from {{ ref('quality_measures__stg_core__lab_result') }}

)

, cholesterol_tests_with_result as (

    select
      labs.person_id
    , labs.result as evidence_value
    , coalesce(collection_date, result_date) as evidence_date
    , cholesterol_codes.concept_name
    , row_number() over (partition by labs.person_id
                          order by labs.result desc
                        , result_date desc) as rn
    from labs
    inner join cholesterol_codes
      on coalesce(labs.normalized_code, labs.source_code) = cholesterol_codes.code
        and coalesce(labs.normalized_code_type, labs.source_code_type) = cholesterol_codes.code_system
   {% if target.type == 'fabric' %}
        WHERE result LIKE '%.%' OR result LIKE '%[0-9]%'
        AND result NOT LIKE '%[^0-9.]%'
    {% else %}
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}
    {% endif %}

)

, cholesterol_labs as (

    select
          person_id
        , evidence_date
    from cholesterol_tests_with_result
    where rn = 1
        and cast(evidence_value as {{ dbt.type_numeric() }}) >= 190

)

, all_patients_with_cholesterol as (

    select
          cholesterol_conditions.person_id
        , cholesterol_conditions.evidence_date
    from cholesterol_conditions

    union all

    select
          cholesterol_procedures.person_id
        , cholesterol_procedures.evidence_date
    from cholesterol_procedures

    union all

    select
          cholesterol_labs.person_id
        , cholesterol_labs.evidence_date
    from cholesterol_labs

)

, patients_with_cholesterol as (

    select
        distinct
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from all_patients_with_cholesterol
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
    from patients_with_cholesterol

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
