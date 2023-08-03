{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}
/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period

    AND either one acute inpatient encounter with a diagnosis of advanced
    illness

    OR two outpatient, observation, ED or nonacute inpatient encounters on
    different dates of service with an advanced illness diagnosis during
    measurement period or the year prior to measurement period
*/

with denominator as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures__int_nqf2372_operational_denominator') }}

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
        , 'Advanced Illness'
        , 'Acute Inpatient'
        , 'Encounter Inpatient'
        , 'Outpatient'
        , 'Observation'
        , 'Emergency Department Visit'
        , 'Nonacute Inpatient'
    )

)

, conditions as (

    select
          patient_id
        , claim_id
        , condition_date
        , code_type
        , code
    from {{ ref('quality_measures__stg_core__condition') }}

)

, medical_claim as (

    select
          patient_id
        , claim_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from {{ ref('quality_measures__stg_medical_claim') }}

)

, procedures as (

    select
          patient_id
        , procedure_date
        , code_type
        , code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.condition_date
        , conditions.code_type
        , conditions.code
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
         on conditions.code = exclusion_codes.code
         and conditions.code_type = exclusion_codes.code_system

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
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

/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period
*/
, patients_with_frailty as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
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
    and condition_exclusions.condition_date
        between denominator.performance_period_begin
        and denominator.performance_period_end

    union all

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
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

/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period

    AND one acute inpatient encounter with a diagnosis of advanced illness
    during measurement period or the year prior to measurement period
*/
, acute_inpatient as (

    select distinct
          patients_with_frailty.patient_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , concat (
              med_claim_exclusions.concept_name
            , ' with '
            , condition_exclusions.concept_name
          ) as exclusion_reason
    from patients_with_frailty
         inner join med_claim_exclusions
         on patients_with_frailty.patient_id = med_claim_exclusions.patient_id
         inner join condition_exclusions
         on med_claim_exclusions.claim_id = condition_exclusions.claim_id
    where med_claim_exclusions.concept_name = 'Acute Inpatient'
    and condition_exclusions.concept_name = 'Advanced Illness'
    and (
        med_claim_exclusions.claim_start_date
            between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
            and patients_with_frailty.performance_period_end
        or med_claim_exclusions.claim_end_date
            between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
            and patients_with_frailty.performance_period_end
    )

)

/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period

    AND two outpatient, observation, ED or nonacute inpatient encounters
    on different dates of service with an advanced illness diagnosis during
    measurement period or the year prior to measurement period
*/
, nonacute_outpatient as (

    select distinct
          patients_with_frailty.patient_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , concat (
              med_claim_exclusions.concept_name
            , ' with '
            , condition_exclusions.concept_name
          ) as exclusion_reason
    from patients_with_frailty
         inner join med_claim_exclusions
         on patients_with_frailty.patient_id = med_claim_exclusions.patient_id
         inner join condition_exclusions
         on med_claim_exclusions.claim_id = condition_exclusions.claim_id
    where med_claim_exclusions.concept_name in (
          'Encounter Inpatient'
        , 'Outpatient'
        , 'Observation'
        , 'Emergency Department Visit'
        , 'Nonacute Inpatient'
    )
    and condition_exclusions.concept_name = 'Advanced Illness'
    and (
        med_claim_exclusions.claim_start_date
            between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
            and patients_with_frailty.performance_period_end
        or med_claim_exclusions.claim_end_date
            between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
            and patients_with_frailty.performance_period_end
    )

)

/*
    Filter to patients who have had one acute inpatient encounter or
    two nonacute outpatient encounters
*/
, acute_inpatient_counts as (

    select
          patient_id
        , count(*) as encounter_count
    from acute_inpatient
    group by patient_id

)

, nonacute_outpatient_counts as (

    select
          patient_id
        , count(*) as encounter_count
    from nonacute_outpatient
    group by patient_id

)

, eligible_acute_inpatient as (

    select
          acute_inpatient.patient_id
        , acute_inpatient.exclusion_date
        , acute_inpatient.exclusion_reason
    from acute_inpatient
         left join acute_inpatient_counts
         on acute_inpatient.patient_id = acute_inpatient_counts.patient_id
    where acute_inpatient_counts.encounter_count >= 1

)

, eligible_nonacute_outpatient as (

    select
          nonacute_outpatient.patient_id
        , nonacute_outpatient.exclusion_date
        , nonacute_outpatient.exclusion_reason
    from nonacute_outpatient
         left join nonacute_outpatient_counts
         on nonacute_outpatient.patient_id = nonacute_outpatient_counts.patient_id
    where nonacute_outpatient_counts.encounter_count >= 2

)

, exclusions_unioned as (

    select
          eligible_acute_inpatient.patient_id
        , eligible_acute_inpatient.exclusion_date
        , eligible_acute_inpatient.exclusion_reason
    from eligible_acute_inpatient

    union all

    select
          eligible_nonacute_outpatient.patient_id
        , eligible_nonacute_outpatient.exclusion_date
        , eligible_nonacute_outpatient.exclusion_reason
    from eligible_nonacute_outpatient

)

select distinct
      patient_id
    , exclusion_date
    , exclusion_reason
from exclusions_unioned