{{ config(
     enabled = var('quality_measures_reporting_enabled',var('claims_enabled',var('tuva_marts_enabled',True)))
   )
}}

/*
    Eligible population from the denominator model before exclusions
*/
with denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures_reporting__int_nqf2372_denominator') }}

)

, mammography_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures_reporting__value_sets') }}
    where concept_name = 'Mammography'

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
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

, qualifying_claims as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
    from medical_claim
         inner join mammography_codes
         on medical_claim.hcpcs_code = mammography_codes.code
    where mammography_codes.code_system = 'hcpcs'

)

, qualifying_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , procedures.code_type
        , procedures.code
    from procedures
         inner join mammography_codes
         on procedures.code = mammography_codes.code
         and procedures.code_type = mammography_codes.code_system

)

/*
    Check if patients in the eligible population have had a screening,
    diagnostic, film, digital or digital breast tomosynthesis (3D)
    mammography results documented and reviewed.
*/

, patients_with_mammograms as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , coalesce(
              qualifying_procedures.procedure_date
            , qualifying_claims.claim_start_date
            , qualifying_claims.claim_end_date
          ) as evidence_date
        , case
            when qualifying_claims.claim_start_date >= denominator.performance_period_begin
                or qualifying_claims.claim_end_date <= denominator.performance_period_end
                then 1
            when qualifying_procedures.procedure_date >= denominator.performance_period_begin
                or qualifying_procedures.procedure_date <= denominator.performance_period_end
                then 1
            else 0
          end as numerator_flag
    from denominator
         left join qualifying_claims
            on denominator.patient_id = qualifying_claims.patient_id
         left join qualifying_procedures
            on denominator.patient_id = qualifying_procedures.patient_id

)

, numerator as (

    select distinct
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , numerator_flag
    from patients_with_mammograms

)

, add_data_types as (

     select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(numerator_flag as integer) as numerator_flag
    from numerator

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , numerator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types