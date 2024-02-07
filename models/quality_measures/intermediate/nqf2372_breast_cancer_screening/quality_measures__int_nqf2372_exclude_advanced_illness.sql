{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
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

with patients_with_frailty as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372__frailty') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
          'Advanced Illness'
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
        , conditions.claim_id
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
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system

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
        , patients_with_frailty.exclusion_reason
            || ' with '
            || med_claim_exclusions.concept_name
            || ' and '
            || condition_exclusions.concept_name
          as exclusion_reason
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

    union all

    select distinct
          patients_with_frailty.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , patients_with_frailty.exclusion_reason
            || ' with '
            || procedure_exclusions.concept_name
            || ' and '
            || condition_exclusions.concept_name
          as exclusion_reason
    from patients_with_frailty
         inner join procedure_exclusions
         on patients_with_frailty.patient_id = procedure_exclusions.patient_id
         inner join condition_exclusions
         on procedure_exclusions.patient_id = condition_exclusions.patient_id
         and procedure_exclusions.procedure_date = condition_exclusions.recorded_date
    where procedure_exclusions.concept_name = 'Acute Inpatient'
    and condition_exclusions.concept_name = 'Advanced Illness'
    and (
        procedure_exclusions.procedure_date
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
        , patients_with_frailty.exclusion_reason
            || ' with '
            || med_claim_exclusions.concept_name
            || ' and '
            || condition_exclusions.concept_name
          as exclusion_reason
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

    union all

    select distinct
          patients_with_frailty.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , patients_with_frailty.exclusion_reason
            || ' with '
            || procedure_exclusions.concept_name
            || ' and '
            || condition_exclusions.concept_name
          as exclusion_reason
    from patients_with_frailty
         inner join procedure_exclusions
         on patients_with_frailty.patient_id = procedure_exclusions.patient_id
         inner join condition_exclusions
         on procedure_exclusions.patient_id = condition_exclusions.patient_id
         and procedure_exclusions.procedure_date = condition_exclusions.recorded_date
    where procedure_exclusions.concept_name in (
          'Encounter Inpatient'
        , 'Outpatient'
        , 'Observation'
        , 'Emergency Department Visit'
        , 'Nonacute Inpatient'
    )
    and condition_exclusions.concept_name = 'Advanced Illness'
    and (
        procedure_exclusions.procedure_date
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
        , count(distinct exclusion_date) as encounter_count
    from acute_inpatient
    group by patient_id

)

, nonacute_outpatient_counts as (

    select
          patient_id
        , count(distinct exclusion_date) as encounter_count
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

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from exclusions_unioned