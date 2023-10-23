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

with encounter_exclusion_codes as (

    select
          code
        , code_system
        , concept_name
        , case when concept_name = 'Acute Inpatient' then 'Acute Inpatient'
            else 'Other Encounter' end as concept_category
        , case when concept_name = 'Acute Inpatient' then 1
            else 2 end as qualifying_count
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
         'Acute Inpatient'
        , 'Encounter Inpatient'
        , 'Outpatient'
        , 'Observation'
        , 'Emergency Department Visit'
        , 'Nonacute Inpatient'
    )

)

, condition_exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
         'Advanced Illness'
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
        , conditions.claim_id
        , conditions.recorded_date
        , condition_exclusion_codes.concept_name
    from conditions
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on conditions.recorded_date between pp.performance_period_begin_1yp and pp.performance_period_end
    inner join condition_exclusion_codes
        on conditions.code = condition_exclusion_codes.code
        and conditions.code_type = condition_exclusion_codes.code_system

    union all
        select
          observations.patient_id
        , cast(null as {{ dbt.type_string() }}) as claim_id
        , observations.observation_date
        , condition_exclusion_codes.concept_name
    from observations
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on observations.observation_date between pp.performance_period_begin_1yp and pp.performance_period_end

    inner join condition_exclusion_codes
        on observations.code = condition_exclusion_codes.code
        and observations.code_type = condition_exclusion_codes.code_system




)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , encounter_exclusion_codes.concept_name
        , encounter_exclusion_codes.concept_category
        , encounter_exclusion_codes.qualifying_count
    from medical_claim
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on coalesce(medical_claim.claim_start_date,medical_claim.claim_end_date) between pp.performance_period_begin_1yp and pp.performance_period_end
         inner join encounter_exclusion_codes
            on medical_claim.hcpcs_code = encounter_exclusion_codes.code
    where encounter_exclusion_codes.code_system = 'hcpcs'

)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , encounter_exclusion_codes.concept_name
        , encounter_exclusion_codes.concept_category
        , encounter_exclusion_codes.qualifying_count
    from observations
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on observations.observation_date between pp.performance_period_begin_1yp and pp.performance_period_end

    inner join encounter_exclusion_codes
        on observations.code = encounter_exclusion_codes.code
        and observations.code_type = encounter_exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , encounter_exclusion_codes.concept_name
        , encounter_exclusion_codes.concept_category
        , encounter_exclusion_codes.qualifying_count
    from procedures
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on procedures.procedure_date between pp.performance_period_begin_1yp and pp.performance_period_end

         inner join encounter_exclusion_codes
             on procedures.code = encounter_exclusion_codes.code
             and procedures.code_type = encounter_exclusion_codes.code_system

)

/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period
*/
, patients_with_frailty as (

    select
          patient_id
        , exclusion_date
        , concept_name
from {{ref('quality_measures__int_nqf0034__frailty')}}


)

/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period

    AND one acute inpatient encounter with a diagnosis of advanced illness
    during measurement period or the year prior to measurement period
*/
, encounters_with_conditions as (

    select distinct
          patients_with_frailty.patient_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , med_claim_exclusions.concept_name
            || ' with '
            || condition_exclusions.concept_name
          as exclusion_reason
        , med_claim_exclusions.concept_category
        , med_claim_exclusions.qualifying_count
    from patients_with_frailty
         inner join med_claim_exclusions
            on patients_with_frailty.patient_id = med_claim_exclusions.patient_id
         inner join condition_exclusions
            on med_claim_exclusions.claim_id = condition_exclusions.claim_id


    union all

    select distinct
          patients_with_frailty.patient_id
        , observation_exclusions.observation_date as exclusion_date
        , observation_exclusions.concept_name as exclusion_reason
        , observation_exclusions.concept_category
        , observation_exclusions.qualifying_count
    from patients_with_frailty
         inner join observation_exclusions
            on patients_with_frailty.patient_id = observation_exclusions.patient_id
         inner join condition_exclusions
             on observation_exclusions.patient_id = condition_exclusions.patient_id
             and observation_exclusions.observation_date = condition_exclusions.recorded_date

    union all

    select distinct
          patients_with_frailty.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
        , procedure_exclusions.concept_category
        , procedure_exclusions.qualifying_count
    from patients_with_frailty
         inner join procedure_exclusions
         on patients_with_frailty.patient_id = procedure_exclusions.patient_id
         inner join condition_exclusions
         on procedure_exclusions.patient_id = condition_exclusions.patient_id
         and procedure_exclusions.procedure_date = condition_exclusions.recorded_date


)

/*
    Patients greater than or equal to 66 with at least one claim/encounter for
    frailty during the measurement period

    AND two outpatient, observation, ED or nonacute inpatient encounters
    on different dates of service with an advanced illness diagnosis during
    measurement period or the year prior to measurement period
*/

/*
    Filter to patients who have had one acute inpatient encounter or
    two nonacute outpatient encounters
*/


, qualifying_encounters as (

    select
      patient_id
    , exclusion_date
    , exclusion_reason
from encounters_with_conditions e
qualify dense_rank() over(partition by patient_id,concept_category order by exclusion_date) >= qualifying_count
)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from qualifying_encounters
