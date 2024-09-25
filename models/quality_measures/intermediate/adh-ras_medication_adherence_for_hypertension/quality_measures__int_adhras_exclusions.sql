{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_adhras__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_adhras__performance_period') }}

)
{%- endset -%}

with denominator as (

    select
          patient_id
    from {{ ref('quality_measures__int_adhras_denominator')}}

)

, valid_hospice_palliative as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
  from {{ref('quality_measures__int_shared_exclusions_hospice_palliative')}}
  where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, esrd_codes as (

    select
            code
          , code_system
          , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name = 'PQA ESRD'

)

, valid_esrd as (

    select
          condition.patient_id
        , condition.recorded_date as exclusion_date
        , esrd_codes.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_core__condition') }} as condition
    inner join esrd_codes 
      on coalesce(condition.normalized_code, condition.source_code) = esrd_codes.code 
        and coalesce(condition.normalized_code_type, condition.source_code_type) = esrd_codes.code_system
    where condition.recorded_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, sacubitril_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name = 'PQA Sacubitril Valsartan Medications'
    
)

, sacubitril_pharmacy_claim as (

    select
          pharmacy_claim.patient_id
        , pharmacy_claim.dispensing_date as exclusion_date
        , sacubitril_codes.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pharmacy_claim
    inner join sacubitril_codes
      on pharmacy_claim.ndc_code = sacubitril_codes.code 
    where pharmacy_claim.dispensing_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, sacubitril_medication as (

    select
          medication.patient_id
        , medication.dispensing_date as exclusion_date
        , sacubitril_codes.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_core__medication') }} as medication
    inner join sacubitril_codes
      on medication.source_code = sacubitril_codes.code 
        and medication.source_code_type = sacubitril_codes.code_system
    where medication.dispensing_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, valid_sacubitril as (

    select 
          patient_id
        , exclusion_date
        , exclusion_reason
    from sacubitril_pharmacy_claim

    union all

    select 
          patient_id
        , exclusion_date
        , exclusion_reason
    from sacubitril_medication

)

, exclusions as (

    select 
          patient_id
        , exclusion_date
        , exclusion_reason
    from valid_hospice_palliative

    union all

    select 
          patient_id
        , exclusion_date
        , exclusion_reason
    from valid_esrd

    union all

    select 
          patient_id
        , exclusion_date
        , exclusion_reason 
    from valid_sacubitril

)

, measure_exclusions as (

    select 
          exclusions.patient_id
        , exclusion_date
        , exclusion_reason
    from exclusions
    inner join denominator
      on exclusions.patient_id = denominator.patient_id
)

, add_data_types as (

  select
      distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , 1 as exclusion_flag
  from measure_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag   
from add_data_types
