{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
 | as_bool
   )
}}

with patients as (

    select
          person_id
    from {{ ref('quality_measures__stg_core__patient') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'frailty device'
        , 'frailty diagnosis'
        , 'frailty encounter'
        , 'frailty symptom'
    )

)

, conditions as (

    select
          person_id
        , recorded_date
        , coalesce(
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__condition') }}

)

, medical_claim as (

    select
          person_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from {{ ref('quality_measures__stg_medical_claim') }}

)

, observations as (

    select
          person_id
        , observation_date
        , coalesce(
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__observation') }}

)

, procedures as (

    select
          person_id
        , procedure_date
        , coalesce(
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, condition_exclusions as (

    select
          conditions.person_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
             on conditions.code = exclusion_codes.code
             and conditions.code_type = exclusion_codes.code_system

)

, med_claim_exclusions as (

    select
          medical_claim.person_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    where exclusion_codes.code_system = 'hcpcs'

)

, observation_exclusions as (

    select
          observations.person_id
        , observations.observation_date
        , exclusion_codes.concept_name
    from observations
         inner join exclusion_codes
             on observations.code = exclusion_codes.code
             and observations.code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.person_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system

)

, patients_with_frailty as (

    select
          patients.person_id
        , condition_exclusions.recorded_date as exclusion_date
        , condition_exclusions.concept_name as exclusion_reason
    from patients
         inner join condition_exclusions
            on patients.person_id = condition_exclusions.person_id

    union all

    select
          patients.person_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , med_claim_exclusions.concept_name as exclusion_reason
    from patients
         inner join med_claim_exclusions
            on patients.person_id = med_claim_exclusions.person_id

    union all

    select
          patients.person_id
        , observation_exclusions.observation_date as exclusion_date
        , observation_exclusions.concept_name as exclusion_reason
    from patients
         inner join observation_exclusions
            on patients.person_id = observation_exclusions.person_id

    union all

    select
          patients.person_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from patients
         inner join procedure_exclusions
            on patients.person_id = procedure_exclusions.person_id

)

select
      person_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from patients_with_frailty
