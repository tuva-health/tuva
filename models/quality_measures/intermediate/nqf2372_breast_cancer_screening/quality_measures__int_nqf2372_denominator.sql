{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with performance_period as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_end
        , performance_period_begin
        , performance_period_lookback
    from {{ ref('quality_measures__int_nqf2372__performance_period') }}

)

, patient as (

    select
          patient_id
        , sex
        , birth_date
        , death_date
    from {{ ref('quality_measures__stg_core__patient') }}

)

, encounters as (

    select
          patient_id
        , encounter_type
        , encounter_start_date
    from {{ ref('quality_measures__stg_core__encounter') }}

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
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

, visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
          'Office Visit'
        , 'Home Healthcare Services'
        , 'Preventive Care Services Established Office Visit, 18 and Up'
        , 'Preventive Care Services Initial Office Visit, 18 and Up'
        , 'Annual Wellness Visit'
        , 'Telephone Visits'
        , 'Online Assessments'
    )

)

, patient_with_age as (

    select
          patient.patient_id
        , patient.sex
        , patient.birth_date
        , patient.death_date
        , performance_period.measure_id
        , performance_period.measure_name
        , performance_period.measure_version
        , performance_period.performance_period_begin
        , performance_period.performance_period_end
        , performance_period.performance_period_lookback
        , floor({{ datediff('patient.birth_date', 'performance_period.performance_period_end', 'hour') }} / 8766.0) as age
    from patient
         cross join performance_period

)

/*
    Filter patient to living women 51 - 74 years of age
    at the beginning of the measurement period
*/
, patient_filtered as (

    select
          patient_id
        , age
        , measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , performance_period_lookback
        , 1 as denominator_flag
    from patient_with_age
    where lower(sex) = 'female'
        and age between 51 and 74
        and death_date is null

)

/*
    Filter to qualifying visit types by claim procedures
*/
, visit_claims as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
    from medical_claim
         inner join visit_codes
            on medical_claim.hcpcs_code = visit_codes.code
    where visit_codes.code_system = 'hcpcs'

)

/*
    Filter encounters to qualifying visit type
*/
, visit_encounters as (

    select
          patient_id
        , encounter_start_date
    from encounters
    where lower(encounter_type) in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
        )

)

/*
    Filter to qualifying visit types by procedure
*/
, visit_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
    from procedures
         inner join visit_codes
             on procedures.code = visit_codes.code
             and procedures.code_type = visit_codes.code_system

)

/*
    Filter to final eligible population/denominator before exclusions
    with a qualifying visit during the measurement period
*/
, eligible_population as (

    select
          patient_filtered.patient_id
        , patient_filtered.age
        , patient_filtered.measure_id
        , patient_filtered.measure_name
        , patient_filtered.measure_version
        , patient_filtered.performance_period_begin
        , patient_filtered.performance_period_end
        , performance_period_lookback
        , patient_filtered.denominator_flag
    from patient_filtered
         left join visit_claims
            on patient_filtered.patient_id = visit_claims.patient_id
         left join visit_procedures
            on patient_filtered.patient_id = visit_procedures.patient_id
         left join visit_encounters
            on patient_filtered.patient_id = visit_encounters.patient_id
    where (
        visit_claims.claim_start_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
        or visit_claims.claim_end_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
        or visit_procedures.procedure_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
        or visit_encounters.encounter_start_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
    )

)

, add_data_types as (

    select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(performance_period_lookback as date) as performance_period_lookback
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from eligible_population

)

 select distinct
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , performance_period_lookback
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types