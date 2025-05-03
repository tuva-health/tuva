{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_nqf0420_denominator') }}

)

, pain_assessment_code as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'pain assessment documented'
    )

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

, pain_assessment_procedures as (

    select
          procedures.person_id
        , procedures.procedure_date as evidence_date
    from procedures
    inner join pain_assessment_code
        on procedures.code = pain_assessment_code.code
            and procedures.code_type = pain_assessment_code.code_system

)

, pain_assessment_claims as (

    select
          person_id
        , coalesce(claim_end_date, claim_start_date) as evidence_date
    from {{ ref('quality_measures__stg_medical_claim') }} as medical_claim
    inner join pain_assessment_code
        on medical_claim.hcpcs_code = pain_assessment_code.code
            and lower(pain_assessment_code.code_system) = 'hcpcs'

)

, time_unbounded_qualifying_patients as (

    select
          person_id
        , evidence_date
    from pain_assessment_procedures

    union all

    select
          person_id
        , evidence_date
    from pain_assessment_claims

)

, qualifying_patients_with_denominator as (

    select
          time_unbounded_qualifying_patients.person_id
        , time_unbounded_qualifying_patients.evidence_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , 1 as numerator_flag
    from time_unbounded_qualifying_patients
    inner join denominator
        on time_unbounded_qualifying_patients.person_id = denominator.person_id
            and time_unbounded_qualifying_patients.evidence_date between
                denominator.performance_period_begin and denominator.performance_period_end

)

, add_data_types as (

     select distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(null as {{ dbt.type_string() }}) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
      from qualifying_patients_with_denominator

)

select
      person_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , evidence_value
    , numerator_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
