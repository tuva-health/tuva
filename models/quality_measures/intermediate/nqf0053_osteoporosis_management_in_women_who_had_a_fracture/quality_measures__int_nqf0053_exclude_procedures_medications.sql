{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
     | as_bool
   )
}}

with denominator as (

    select
          patient_id
        , performance_period_begin
    from {{ ref('quality_measures__int_nqf0053_denominator')}}

)

, value_sets as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('quality_measures__value_sets')}}

)

, procedures as (

    select
          patient_id
        , encounter_id
        , procedure_date
        , source_code
        , source_code_type
    from {{ ref('quality_measures__stg_core__procedure')}}

)

, pharmacy_claims as (

    select
          patient_id
        , dispensing_date
        , ndc_code
    from {{ ref('quality_measures__stg_pharmacy_claim') }}

)

, medications as (

    select
        patient_id
      , encounter_id
      , prescribing_date
      , dispensing_date
      , source_code
      , source_code_type
    from {{ ref('quality_measures__stg_core__medication') }}

)

, bone_density_test_codes as (

    select
          concept_name
        , code
        , code_system
    from value_sets
    where lower(concept_name) in (
          'bone mineral density test'
        , 'bone mineral density tests cpt'
        , 'bone mineral density tests hcpcs'
        , 'bone mineral density tests icd10pcs'
        , 'dexa dual energy xray absorptiometry, bone density'
    )

)

, osteoporosis_medication_codes as (

    select
          code
        , code_system
        , concept_name
    from value_sets
    where lower(concept_name) in 
        ( 
          'osteoporosis medications for urology care'
        , 'osteoporosis medication'
        , 'bisphosphonates'
        )

)

, bone_density_test_procedures as (

    select
          procedures.*
        , bone_density_test_codes.concept_name
    from procedures
    inner join bone_density_test_codes
        on procedures.source_code = bone_density_test_codes.code
            and procedures.source_code_type = bone_density_test_codes.code_system

)

, osteoporosis_pharmacy_claims as (

    select
        pharmacy_claims.patient_id
      , pharmacy_claims.dispensing_date
      , pharmacy_claims.ndc_code
      , osteoporosis_medication_codes.concept_name
    from pharmacy_claims
    inner join osteoporosis_medication_codes
        on pharmacy_claims.ndc_code = osteoporosis_medication_codes.code
            and lower(osteoporosis_medication_codes.code_system) = 'ndc'
            
)

, osteoporosis_medications as (

    select
        medications.patient_id
      , medications.encounter_id
      , medications.prescribing_date
      , medications.dispensing_date
      , medications.source_code
      , medications.source_code_type
      , osteoporosis_medication_codes.concept_name
    from medications
    inner join osteoporosis_medication_codes
        on medications.source_code = osteoporosis_medication_codes.code
            and medications.source_code_type = osteoporosis_medication_codes.code_system

)

, valid_osteoporosis_medications_procedures as (

    select
          denominator.patient_id
        , osteoporosis_pharmacy_claims.concept_name as exclusion_reason
        , osteoporosis_pharmacy_claims.dispensing_date as exclusion_date
    from denominator
    inner join osteoporosis_pharmacy_claims
        on denominator.patient_id = osteoporosis_pharmacy_claims.patient_id
    where osteoporosis_pharmacy_claims.dispensing_date
        between
            {{ dbt.dateadd (
                      datepart = "month"
                    , interval = -12
                    , from_date_or_timestamp = "denominator.performance_period_begin"
            )}}
            and denominator.performance_period_begin
    
    union all

    select
          denominator.patient_id
        , osteoporosis_medications.concept_name as exclusion_reason
        , coalesce(osteoporosis_medications.prescribing_date, osteoporosis_medications.dispensing_date) as exclusion_date
    from denominator
    inner join osteoporosis_medications
        on denominator.patient_id = osteoporosis_medications.patient_id
            and coalesce(osteoporosis_medications.prescribing_date, osteoporosis_medications.dispensing_date)
            between
                {{ dbt.dateadd (
                        datepart = "month"
                        , interval = -12
                        , from_date_or_timestamp = "denominator.performance_period_begin"
                )}}
                and denominator.performance_period_begin

)

, valid_tests_performed as (

    select
          denominator.patient_id
        , bone_density_test_procedures.concept_name as exclusion_reason
        , procedure_date as exclusion_date
    from denominator
    inner join bone_density_test_procedures
        on denominator.patient_id = bone_density_test_procedures.patient_id
    where bone_density_test_procedures.procedure_date
        between 
            {{ dbt.dateadd (
                      datepart = "year"
                    , interval = -2
                    , from_date_or_timestamp = "denominator.performance_period_begin"
            )}}
            and denominator.performance_period_begin

)

, valid_exclusions as (

    select * from valid_tests_performed

    union all

    select * from valid_osteoporosis_medications_procedures

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , 'measure specific exclusion for procedure medication' as exclusion_type
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from
    valid_exclusions