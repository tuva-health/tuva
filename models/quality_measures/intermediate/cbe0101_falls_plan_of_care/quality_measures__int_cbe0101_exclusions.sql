{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(

  select 
    performance_period_begin
  from {{ ref('quality_measures__int_cbe0101__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(

  select 
    performance_period_end
  from {{ ref('quality_measures__int_cbe0101__performance_period') }}

)
{%- endset -%}

with denominator as (
    
    select
        patient_id
    from {{ ref('quality_measures__int_cbe0101_denominator') }}

)

, exclusion_code as (

    select
        code
      , code_system
    from {{ ref('quality_measures__value_sets') }}
    where code = '0518F'
    -- further 1P modifier are only excluded
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

, valid_hospice as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from hospice_palliative
    where exclusion_type in (
            'hospice encounter'
          , 'hospice care ambulatory'
          , 'hospice diagnosis'
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
        , modifier_1
        , modifier_2
        , modifier_3
        , modifier_4
        , modifier_5
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, exclusion_procedures as (

    select
          patient_id
        , procedure_date as exclusion_date
        , 'Limited mobility' as exclusion_reason
    from procedures
    inner join exclusion_code
        on procedures.code = exclusion_code.code
            and procedures.code_type = exclusion_code.code_system
    where '1P' in (modifier_1, modifier_2, modifier_3, modifier_4, modifier_5)
            
)

, exclusion_claims as (

    select
          patient_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , 'Limited mobility' as exclusion_reason
    from {{ ref('quality_measures__stg_medical_claim') }} medical_claim
    inner join exclusion_code
        on medical_claim.hcpcs_code = exclusion_code.code
            and lower(exclusion_code.code_system) = 'hcpcs'
    where '1P' in (hcpcs_modifier_1, hcpcs_modifier_2, hcpcs_modifier_3, hcpcs_modifier_4, hcpcs_modifier_5)

)

, exclusion_patients as (

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from valid_hospice

    union all

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from exclusion_procedures

    union all

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from exclusion_claims

)

, add_data_types as (

    select
        distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(1 as integer) as exclusion_flag
    from exclusion_patients

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
