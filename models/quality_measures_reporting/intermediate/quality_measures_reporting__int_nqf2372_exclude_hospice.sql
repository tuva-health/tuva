{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}

/*
    Hospice services used by patient any time during the measurement period
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
          'Hospice Care Ambulatory'
        , 'Hospice Encounter'
    )

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from {{ ref('quality_measures_reporting__stg_medical_claim') }}

)

, procedures as (

    select
          patient_id
        , procedure_date
        , code_type
        , code
    from {{ ref('quality_measures_reporting__stg_core__procedure') }}

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

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , procedures.code_type
        , procedures.code
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
         on procedures.code = exclusion_codes.code
         and procedures.code_type = exclusion_codes.code_system

)

, hospice_claims as (

    select
          denominator.patient_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , med_claim_exclusions.concept_name as exclusion_reason
    from denominator
         inner join med_claim_exclusions
         on denominator.patient_id = med_claim_exclusions.patient_id
    where (
        med_claim_exclusions.claim_start_date
            between denominator.performance_period_begin
            and denominator.performance_period_end
        or med_claim_exclusions.claim_end_date
            between denominator.performance_period_begin
            and denominator.performance_period_end
    )

)

, hospice_procedures as (

    select
          denominator.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from denominator
         inner join procedure_exclusions
         on denominator.patient_id = procedure_exclusions.patient_id
    where procedure_exclusions.procedure_date
        between denominator.performance_period_begin
        and denominator.performance_period_end

)

, exclusions_unioned as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from hospice_claims

    union all

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from hospice_procedures

)

select distinct
      patient_id
    , exclusion_date
    , exclusion_reason
from exclusions_unioned