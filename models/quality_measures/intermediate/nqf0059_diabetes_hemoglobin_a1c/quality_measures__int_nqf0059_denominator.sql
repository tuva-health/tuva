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
        'Annual Wellness Visit'
        ,'Home Healthcare Services'
        ,'Office Visit'
        ,'Outpatient'
    )

)   

, visit_encounters as (

    select
          patient_id
        , encounter_type
        , encounter_start_date
    from {{ ref('quality_measures__stg_core__encounter') }}
    -- where lower(encounter_type) in (
    --       'home health'
    --     , 'office visit'
    --     , 'outpatient'
    --     , 'outpatient rehabilitation'
    -- ) TBD
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
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from {{ ref('quality_measures__stg_core__condition')}}

)

, diabetic_conditions as (

    select
          patient_id
        , claim_id
        , recorded_date
        , source_code
        , source_code_type
    from conditions
    inner join diabetics_codes
        on conditions.source_code_type = diabetics_codes.code_system
            and conditions.source_code = diabetics_codes.code

)

, patient_with_age as (

    select
          visit_encounters.patient_id
        , visit_encounters.encounter_type
        , visit_encounters.encounter_start_date
        , patients.birth_date
        , floor({{ datediff('patients.birth_date', 'visit_encounters.encounter_start_date', 'hour') }} / 8760.0) as age
    from visit_encounters
    left join patients
        on visit_encounters.patient_id = patients.patient_id
    where patients.death_date is null

)

, eligible_population as (

    select
          diabetic_conditions.patient_id
        , diabetic_conditions.claim_id
        , diabetic_conditions.recorded_date
        , patient_with_age
    from diabetic_conditions
    inner join patient_with_age
        on diabetic_conditions.patient_id = patient_with_age.patient_id


)


select * from patient_with_age
