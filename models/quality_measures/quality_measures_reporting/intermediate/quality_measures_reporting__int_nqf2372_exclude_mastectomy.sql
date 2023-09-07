{{ config(
     enabled = var('quality_measures_reporting_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

/*
    Patients that had a mastectomy performed or who have a history of mastectomy
*/

with denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures_reporting__int_nqf2372_denominator') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures_reporting__value_sets') }}
    where concept_name in (
          'Bilateral Mastectomy'
        , 'History of bilateral mastectomy'
        , 'Status Post Left Mastectomy'
        , 'Status Post Right Mastectomy'
        , 'Unilateral Mastectomy Left'
        , 'Unilateral Mastectomy Right'
        , 'Unilateral Mastectomy, Unspecified Laterality'
    )

)

, conditions as (

    select
          patient_id
        , recorded_date
        , normalized_code_type
        , normalized_code
    from {{ ref('quality_measures_reporting__stg_core__condition') }}

)

, procedures as (

    select
          patient_id
        , procedure_date
        , normalized_code_type
        , normalized_code
    from {{ ref('quality_measures_reporting__stg_core__procedure') }}

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.normalized_code_type
        , conditions.normalized_code
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
         on conditions.normalized_code = exclusion_codes.code
         and conditions.normalized_code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , procedures.normalized_code_type
        , procedures.normalized_code
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
         on procedures.normalized_code = exclusion_codes.code
         and procedures.normalized_code_type = exclusion_codes.code_system

)

, mastectomy_conditions as (

    select
          denominator.patient_id
        , condition_exclusions.recorded_date as exclusion_date
        , condition_exclusions.concept_name as exclusion_reason
    from denominator
         inner join condition_exclusions
         on denominator.patient_id = condition_exclusions.patient_id

)

, mastectomy_procedures as (

    select
          denominator.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from denominator
         inner join procedure_exclusions
         on denominator.patient_id = procedure_exclusions.patient_id

)

, exclusions_unioned as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from mastectomy_conditions

    union all

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from mastectomy_procedures

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
from exclusions_unioned