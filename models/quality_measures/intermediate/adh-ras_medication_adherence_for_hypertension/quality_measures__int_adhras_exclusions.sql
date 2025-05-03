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
          person_id
    from {{ ref('quality_measures__int_adhras_denominator') }}

)

, valid_hospice_palliative as (

    select
        person_id
      , exclusion_date
      , exclusion_reason
    from {{ ref('quality_measures__int_shared_exclusions_hospice_palliative') }}
    where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, codes as (

    select
            code
          , code_system
          , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
            'PQA ESRD'
          , 'PQA Sacubitril Valsartan Medications'
        )

)

, valid_esrd as (

    select
          condition.person_id
        , condition.recorded_date as exclusion_date
        , codes.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_core__condition') }} as condition
    inner join codes
      on coalesce(condition.normalized_code, condition.source_code) = codes.code
        and coalesce(condition.normalized_code_type, condition.source_code_type) = codes.code_system
    where condition.recorded_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, sacubitril_pharmacy_claim as (

    select
          pharmacy_claim.person_id
        , pharmacy_claim.dispensing_date as exclusion_date
        , codes.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pharmacy_claim
    inner join codes
      on pharmacy_claim.ndc_code = codes.code
    where pharmacy_claim.dispensing_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, exclusions as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from valid_hospice_palliative

    union all

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from valid_esrd

    union all

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from sacubitril_pharmacy_claim

)

, measure_exclusions as (

    select
          exclusions.person_id
        , exclusion_date
        , exclusion_reason
    from exclusions
    inner join denominator
      on exclusions.person_id = denominator.person_id

)

, add_data_types as (

    select
        distinct
            cast(person_id as {{ dbt.type_string() }}) as person_id
          , cast(exclusion_date as date) as exclusion_date
          , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
          , 1 as exclusion_flag
    from measure_exclusions

)

select
      person_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
