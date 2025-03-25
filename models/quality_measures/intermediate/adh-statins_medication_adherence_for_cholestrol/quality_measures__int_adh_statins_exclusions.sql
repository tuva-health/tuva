{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_adh_statins__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_adh_statins__performance_period') }}

)
{%- endset -%}

with denominator as (

    select
          person_id
    from {{ ref('quality_measures__int_adh_statins_denominator') }}

)

, hospice_palliative as (

    select
        person_id
      , exclusion_date
      , exclusion_reason
    from {{ ref('quality_measures__int_shared_exclusions_hospice_palliative') }}
    where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, valid_hospice as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from hospice_palliative
    where lower(exclusion_reason) in (
            'hospice encounter'
          , 'hospice care ambulatory'
          , 'hospice diagnosis'
    )

)

, esrd_codes as (

    select
            code
          , code_system
          , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
            'PQA ESRD'
        )

)

, valid_esrd as (

    select
          condition.person_id
        , condition.recorded_date as exclusion_date
        , esrd_codes.concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_core__condition') }} as condition
    inner join esrd_codes
      on coalesce(condition.normalized_code, condition.source_code) = esrd_codes.code
        and coalesce(condition.normalized_code_type, condition.source_code_type) = esrd_codes.code_system
    where condition.recorded_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, exclusions as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from valid_hospice

    union all

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from valid_esrd

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
