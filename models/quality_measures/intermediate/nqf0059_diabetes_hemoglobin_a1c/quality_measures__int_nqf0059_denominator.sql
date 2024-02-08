{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}
with performance_period as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures__int_nqf0059__performance_period')}}

)

, patients as (

    select
          patient_id
        , birth_date
        , death_date
    from {{ ref('quality_measures__stg_core__patient') }}

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
    )

)   

, visit_encounters as (

    select
          patient_id
        , encounter_id
        , encounter_type
        , encounter_start_date
    from {{ ref('quality_measures__stg_core__encounter') }}
    -- where lower(encounter_type) in (
    --       'home health'
    --     , 'office visit'
    --     , 'outpatient'
    --     , 'outpatient rehabilitation'
    -- )
)

, visit_claims as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
    from medical_claim
        inner join visit_codes
            on medical_claim.hcpcs_code = visit_codes.code
    where visit_codes.code_system = 'hcps'

)

, visit_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
    from procedures
         inner join visit_codes
             on procedures.code = visit_codes.code
             and procedures.code_type = visit_codes.code_system

)

, diabetics_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
        'Diabetes',
        'HbA1c Laboratory Test',
        'Nutrition Services'
    )
)

, conditions as (

    select
          patient_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from {{ ref('quality_measures__stg_core__condition')}}

)

, diabetic_conditions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.encounter_id
        , conditions.recorded_date
        , conditions.source_code
        , conditions.source_code_type
    from conditions
    inner join diabetics_codes
        on conditions.source_code_type = diabetics_codes.code_system
            and conditions.source_code = diabetics_codes.code

)

, patients_with_age as (

    select
          visit_encounters.patient_id
        , visit_encounters.encounter_id
        , visit_encounters.encounter_type
        , visit_encounters.encounter_start_date
        , performance_period.measure_id
        , performance_period.measure_name
        , performance_period.measure_version
        , performance_period.performance_period_begin
        , performance_period.performance_period_end
        , patients.birth_date
        , floor({{ datediff('patients.birth_date', 'performance_period.performance_period_end', 'hour') }} / 8760.0) as age
    from visit_encounters
    left join patients
        on visit_encounters.patient_id = patients.patient_id
    cross join performance_period
    where patients.death_date is null

)

, filtered_patients as (

    select
          diabetic_conditions.*
        , patients_with_age.measure_id
        , patients_with_age.measure_name
        , patients_with_age.measure_version
        , patients_with_age.encounter_type
        , patients_with_age.encounter_start_date
        , patients_with_age.performance_period_begin
        , patients_with_age.performance_period_end
        , patients_with_age.age
        , 1 as denominator_flag
    from diabetic_conditions
    left join patients_with_age
        on diabetic_conditions.encounter_id = patients_with_age.encounter_id
            and diabetic_conditions.patient_id = patients_with_age.patient_id
    where age between 18 and 75

)

, eligible_population as (

   select
          filtered_patients.patient_id
        , filtered_patients.age
        , filtered_patients.measure_id
        , filtered_patients.measure_name
        , filtered_patients.measure_version
        , filtered_patients.performance_period_begin
        , filtered_patients.performance_period_end
        , filtered_patients.denominator_flag
    from filtered_patients
    left join visit_claims
        on filtered_patients.patient_id = visit_claims.patient_id
    left join visit_procedures
        on filtered_patients.patient_id = visit_procedures.patient_id
    left join visit_encounters
        on filtered_patients.patient_id = visit_encounters.patient_id
    where 
    (
        visit_claims.claim_start_date
            between filtered_patients.performance_period_begin
            and filtered_patients.performance_period_end
        or visit_claims.claim_end_date
            between filtered_patients.performance_period_begin
            and filtered_patients.performance_period_end
        or visit_procedures.procedure_date
            between filtered_patients.performance_period_begin
            and filtered_patients.performance_period_end
        or visit_encounters.encounter_start_date
            between filtered_patients.performance_period_begin
            and filtered_patients.performance_period_end
    )
   

)

, add_data_types as (

    select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
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
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
