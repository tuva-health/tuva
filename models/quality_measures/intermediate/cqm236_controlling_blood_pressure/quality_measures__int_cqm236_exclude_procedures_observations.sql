{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',false))))
    | as_bool
   )
}}

with denominator as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures__int_cqm236_denominator') }}

)


, conditions as (

    select
          patient_id
        , claim_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__condition') }}

)

, procedures as (

    select
          patient_id
        , procedure_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets')}}
    where lower(concept_name) in (
          'dialysis services'
        , 'end stage renal disease'
        , 'esrd monthly outpatient services'
        , 'kidney transplant'
        , 'kidney transplant recipient'
        , 'pregnancy'
    )

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system

)

, exclusions_unioned as (

    select
          patient_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select
          patient_id
        , procedure_date as exclusion_date
        , concept_name as exclusion_reason
    from procedure_exclusions

)

, excluded_patients as (

    select
          exclusions_unioned.*
        , case
            when exclusion_reason = 'pregnancy' then 1
            else 0
          end as is_pregnant
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.age
    from exclusions_unioned
    inner join denominator
        on exclusions_unioned.patient_id = denominator.patient_id

)

, exclusions_filtered as (

    select
        *
    from excluded_patients
    where is_pregnant = 1
        and exclusion_date between performance_period_begin and performance_period_end
    
    union all

    select
        *
    from excluded_patients
    where is_pregnant = 0
        and exclusion_date between performance_period_begin and performance_period_end
          or (exclusion_date between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="performance_period_begin") }}
            and performance_period_end)

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , age
    , 'measure specific exclusion for observation procedure' as exclusion_type
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from
    exclusions_filtered
