{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
DENOMINATOR EXCLUSIONS:
Patients 66 years of age and older with at least one claim/encounter
for frailty during the measurement period (not full exclusion, used
in conjunction with dementia medication or
*/


with aged_patients as (
    select distinct patient_id
    from {{ref('quality_measures__int_nqf0034_denominator')}}
    where max_age >=66

)

, exclusion_codes as (
    select
          code
        , Descriptor
        , case CodeSystemName
            when 'SNOMEDCT' then 'snomed-ct'
            when 'ICD9CM' then 'icd-9-cm'
            when 'ICD10CM' then 'icd-10-cm'
            when 'CPT' then 'cpt'
            when 'ICD10PCS' then 'icd-10-pcs'
          else lower(codesystemname) end as code_system
        , ValueSetName
        , 0 as meets_all_criteria
    From {{ref('quality_measures__value_set_codes')}}
    where ValueSetName in  (
          'Frailty Device'
        , 'Frailty Diagnosis'
        , 'Frailty Encounter'
        , 'Frailty Symptom'
    )
    union all
    select
          'G2100' as code
        , 'Patient 66 or older with a claim/encounter for frailty and dispensed medication for dementia' as descriptor
        , 'hcpcs' as code_system
        , 'nqf past history' as valuesetname
        , 1 as meets_all_criteria
    union all
    select
          'G2101' as code
        , 'Patient 66 or older with a claim/encounter for frailty and one acute inpatient or two outpatient encounters for advanced illness' as descriptor
        , 'hcpcs' as code_system
        , 'nqf past history' as valuesetname
        , 1 as meets_all_criteria




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
 -- todo - should we qualify on normalized code AND (really OR) source code instaed of a coalesce?


)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.ValueSetName
    from conditions
        inner join aged_patients
            on conditions.patient_id = aged_patients.patient_id
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp on
        recorded_date between pp.performance_period_begin and pp.performance_period_end

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.ValueSetName
    from medical_claim
        inner join aged_patients
            on medical_claim.patient_id = aged_patients.patient_id
        inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
        inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp
            on claim_start_date between pp.performance_period_begin and pp.performance_period_end
    where exclusion_codes.code_system = 'hcpcs'


)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.ValueSetName
    from observations
    inner join aged_patients
        on observations.patient_id = aged_patients.patient_id
    inner join exclusion_codes
        on observations.code = exclusion_codes.code
        and observations.code_type = exclusion_codes.code_system
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp
        on observation_date between pp.performance_period_begin and pp.performance_period_end

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.ValueSetName
    from procedures
    inner join aged_patients
        on procedures.patient_id = aged_patients.patient_id
    inner join exclusion_codes
        on procedures.code = exclusion_codes.code
        and procedures.code_type = exclusion_codes.code_system
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp on
       procedure_date between pp.performance_period_begin and pp.performance_period_end

)

, patients_with_exclusions as(
    select patient_id
      , recorded_date as exclusion_date
      , valuesetname as concept_name
    from condition_exclusions

    union distinct

    select patient_id
      , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
      , valuesetname as concept_name
    from med_claim_exclusions

    union distinct

    select patient_id
      , observation_date as exclusion_date
      , valuesetname as concept_name
    from observation_exclusions

    union distinct

    select patient_id
      , procedure_date as exclusion_date
      , valuesetname as concept_name
    from procedure_exclusions

)
, ordered_exclusions as (
    select patient_id
        , exclusion_date
        , concept_name
        , row_number() over (partition by patient_id order by exclusion_date) as exclusion_row
    from patients_with_exclusions
    )

select  patient_id
    , exclusion_date
    , concept_name
from ordered_exclusions