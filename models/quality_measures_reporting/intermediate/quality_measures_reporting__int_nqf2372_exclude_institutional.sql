{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}

/*
    Patients greater than or equal to 66 in Institutional Special Needs Plans (SNP)
    or residing in long term care

    Future enhancement: group claims into encounters
*/

with denominator as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures_reporting__int_nqf2372_denominator') }}

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

, exclusions as (

    select
          denominator.patient_id
        , coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) as exclusion_date
        , 'Institutional or Long Term Care' as exclusion_reason
    from denominator
         inner join medical_claim
         on denominator.patient_id = medical_claim.patient_id
    where denominator.age >= 66
    and (
        medical_claim.claim_start_date
            between denominator.performance_period_begin
            and denominator.performance_period_end
        or medical_claim.claim_end_date
            between denominator.performance_period_begin
            and denominator.performance_period_end
    )
    and place_of_service_code in ('32', '33', '34', '54', '56')
    and {{ datediff('medical_claim.claim_start_date', 'medical_claim.claim_end_date', 'day') }} >= 90

)

select distinct
      patient_id
    , exclusion_date
    , exclusion_reason
from exclusions