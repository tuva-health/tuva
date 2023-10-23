{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
DENOMINATOR EXCLUSIONS:
Patient was provided hospice services any time during the measurement period: G9710
*/


with exclusion_codes as (
    select
          code
        , case code_system
            when 'SNOMEDCT' then 'snomed-ct'
            when 'ICD9CM' then 'icd-9-cm'
            when 'ICD10CM' then 'icd-10-cm'
            when 'CPT' then 'hcpcs'
            when 'ICD10PCS' then 'icd-10-pcs'
          else lower(code_system) end as code_system
        , concept_name
    From {{ref('quality_measures__value_sets')}}
    where concept_name in  (
          'Hospice Care Ambulatory'
        , 'Hospice Encounter'
        , 'Palliative Care Encounter'
        , 'Palliative Care Intervention'
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
    from {{ ref('quality_measures__stg_core__condition') }} )

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
        , exclusion_codes.concept_name as concept_name
    from conditions
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp
        on recorded_date between pp.performance_period_begin and pp.performance_period_end

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name as concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp on 1=1
    where exclusion_codes.code_system = 'hcpcs'
    and claim_start_date between pp.performance_period_begin and pp.performance_period_end
)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name as concept_name
    from observations
    inner join exclusion_codes
        on observations.code = exclusion_codes.code
        and observations.code_type = exclusion_codes.code_system
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp on 1=1
    where observation_date between pp.performance_period_begin and pp.performance_period_end

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name as concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp on 1=1
    where procedure_date between pp.performance_period_begin and pp.performance_period_end

)

, patients_with_exclusions as(
    select patient_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select patient_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , concept_name as exclusion_reason
    from med_claim_exclusions

    union all

    select patient_id
        , observation_date as exclusion_date
        , concept_name as exclusion_reason
    from observation_exclusions

    union all

    select patient_id
        , procedure_date as exclusion_date
        , concept_name as exclusion_reason
    from procedure_exclusions

)



select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from patients_with_exclusions