{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(

  select 
    performance_period_begin
  from {{ ref('quality_measures__int_nqf0041__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(

  select 
    performance_period_end
  from {{ ref('quality_measures__int_nqf0041__performance_period') }}

)
{%- endset -%}

with denominator as (
    
    select
        patient_id
    from {{ ref('quality_measures__int_nqf0041_denominator') }}

)

, exclusion_codes as (

  select 
      code
    , code_system
    , concept_name
  from {{ ref('quality_measures__value_sets') }}
  where lower(concept_name) in (
        'influenza immunization not ordered or administered reason documented'
    )

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
    where procedure_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
    from {{ ref('quality_measures__stg_medical_claim') }}
    where coalesce(claim_end_date, claim_start_date) between {{ performance_period_begin }} and {{ performance_period_end }}

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

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , coalesce(medical_claim.claim_end_date, medical_claim.claim_start_date) as exclusion_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name as concept_name
    from medical_claim
    inner join exclusion_codes
      on medical_claim.hcpcs_code = exclusion_codes.code
        and exclusion_codes.code_system = 'hcpcs'

)

, hospice_palliative as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
        , exclusion_type
    from {{ ref('quality_measures__int_shared_exclusions_hospice_palliative') }}
    where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, valid_hospice_palliative as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
        , exclusion_type
    from hospice_palliative
    where exclusion_reason in (
          'hospice care ambulatory'
        , 'hospice encounter'
    )

)

, valid_exclusions as (

    select
        patient_id
      , procedure_date as exclusion_date
      , concept_name as exclusion_reason
    from procedure_exclusions

    union all

    select
        patient_id
      , exclusion_date
      , concept_name as exclusion_reason
    from med_claim_exclusions

    union all

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from valid_hospice_palliative

)

, combined_exclusions as (

    select
        valid_exclusions.patient_id
      , valid_exclusions.exclusion_date
      , valid_exclusions.exclusion_reason
    from valid_exclusions
    inner join denominator
      on valid_exclusions.patient_id = denominator.patient_id

)

, add_data_types as (

    select
        distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(1 as integer) as exclusion_flag
    from combined_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
