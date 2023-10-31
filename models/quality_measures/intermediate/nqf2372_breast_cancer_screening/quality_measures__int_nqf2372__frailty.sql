{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}
/*
    This concept is used in multiple exclusions. Patients greater than
    or equal to 66 with at least one claim/encounter for frailty during
    the measurement period.
*/
with denominator as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures__int_nqf2372_denominator') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
          'Frailty Device'
        , 'Frailty Diagnosis'
        , 'Frailty Encounter'
        , 'Frailty Symptom'
    )

)

, conditions as (

    select
          patient_id
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

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from {{ ref('quality_measures__stg_medical_claim') }}

)

, observations as (

    select
          patient_id
        , observation_date
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
    from {{ ref('quality_measures__stg_core__observation') }}

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

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
             on conditions.code = exclusion_codes.code
             and conditions.code_type = exclusion_codes.code_system

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    where exclusion_codes.code_system = 'hcpcs'

)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name
    from observations
         inner join exclusion_codes
             on observations.code = exclusion_codes.code
             and observations.code_type = exclusion_codes.code_system

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

, patients_with_frailty as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , condition_exclusions.recorded_date as exclusion_date
        , condition_exclusions.concept_name as exclusion_reason
    from denominator
         inner join condition_exclusions
            on denominator.patient_id = condition_exclusions.patient_id
    where denominator.age >= 66
        and condition_exclusions.concept_name in (
              'Frailty Device'
            , 'Frailty Diagnosis'
            , 'Frailty Encounter'
            , 'Frailty Symptom'
        )
        and condition_exclusions.recorded_date
            between denominator.performance_period_begin
            and denominator.performance_period_end

    union all

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , med_claim_exclusions.concept_name as exclusion_reason
    from denominator
         inner join med_claim_exclusions
            on denominator.patient_id = med_claim_exclusions.patient_id
    where denominator.age >= 66
        and med_claim_exclusions.concept_name in (
              'Frailty Device'
            , 'Frailty Diagnosis'
            , 'Frailty Encounter'
            , 'Frailty Symptom'
        )
        and (
            med_claim_exclusions.claim_start_date
                between denominator.performance_period_begin
                and denominator.performance_period_end
            or med_claim_exclusions.claim_end_date
                between denominator.performance_period_begin
                and denominator.performance_period_end
        )

    union all

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , observation_exclusions.observation_date as exclusion_date
        , observation_exclusions.concept_name as exclusion_reason
    from denominator
         inner join observation_exclusions
            on denominator.patient_id = observation_exclusions.patient_id
    where denominator.age >= 66
        and observation_exclusions.concept_name in (
              'Frailty Device'
            , 'Frailty Diagnosis'
            , 'Frailty Encounter'
            , 'Frailty Symptom'
        )
        and observation_exclusions.observation_date
            between denominator.performance_period_begin
            and denominator.performance_period_end

    union all

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from denominator
         inner join procedure_exclusions
            on denominator.patient_id = procedure_exclusions.patient_id
    where denominator.age >= 66
        and procedure_exclusions.concept_name in (
              'Frailty Device'
            , 'Frailty Diagnosis'
            , 'Frailty Encounter'
            , 'Frailty Symptom'
        )
        and procedure_exclusions.procedure_date
            between denominator.performance_period_begin
            and denominator.performance_period_end

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from patients_with_frailty