{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',false))))
   )
}}

/*
    patients greater than or equal to 66 in institutional special needs plans (snp)
    or residing in long term care

    future enhancement: group claims into encounters
*/

with denominator as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures__int_nqf0059_denominator') }}

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
          denominator.patient_id
        , coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) as exclusion_date
        , 'institutional or long term care' as exclusion_reason
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

, add_data_types as (

    select
       cast(patient_id as {{ dbt.type_string() }}) as patient_id
     , cast(exclusion_date as date) as exclusion_date
     , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
    from exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
