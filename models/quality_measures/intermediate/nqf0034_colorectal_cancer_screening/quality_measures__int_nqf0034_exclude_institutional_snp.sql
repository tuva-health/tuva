{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
    Patients greater than or equal to 66 in Institutional Special Needs Plans (SNP)
    or residing in long term care

    Future enhancement: group claims into encounters
*/

with aged_patients as (
    select distinct patient_id
    from {{ref('quality_measures__int_nqf0034_denominator')}}
    where max_age >=66

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

, exclusions as (

    select
          aged_patients.patient_id
        , coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) as exclusion_date
        , 'Institutional or Long Term Care' as exclusion_reason
    from aged_patients
         inner join medical_claim
         on aged_patients.patient_id = medical_claim.patient_id

    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) between pp.performance_period_begin and pp.performance_period_end

    where place_of_service_code in ('32', '33', '34', '54', '56')
    and {{ datediff('medical_claim.claim_start_date', 'medical_claim.claim_end_date', 'day') }} >= 90
)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from exclusions