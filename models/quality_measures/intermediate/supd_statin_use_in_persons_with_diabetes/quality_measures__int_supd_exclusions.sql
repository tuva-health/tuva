{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(

  select 
    performance_period_begin
  from {{ ref('quality_measures__int_supd__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(

  select 
    performance_period_end
  from {{ ref('quality_measures__int_supd__performance_period') }}

)
{%- endset -%}

with denominator as (

    select
        person_id
    from {{ ref('quality_measures__int_supd_denominator') }}

)

, exclusion_codes as (

    select
        code
      , code_system
      , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'pqa esrd'
        , 'pqa rhabdomyolysis myopathy'
        , 'pqa pregnancy'
        , 'pqa lactation'
        , 'pqa fertility medications'
        , 'pqa cirrhosis'
        , 'pqa pcos'
        , 'pqa prediabetes'
    )

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

, conditions as (

    select
          person_id
        , recorded_date
        , claim_id
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
    where recorded_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, condition_exclusions as (

      select
          conditions.person_id
        , conditions.recorded_date as exclusion_date
        , exclusion_codes.concept_name as exclusion_reason
    from conditions
    inner join exclusion_codes
      on conditions.code = exclusion_codes.code
        and conditions.code_type = exclusion_codes.code_system

)

, pharmacy_exclusions as (

    select
        person_id
      , dispensing_date as exclusion_date
      , concept_name as exclusion_reason
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pharmacy_claims
    inner join exclusion_codes
        on exclusion_codes.code_system = 'ndc'
          and pharmacy_claims.ndc_code = exclusion_codes.code

)

, exclusion_patients as (

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
    from condition_exclusions

    union all

    select
        person_id
      , exclusion_date
      , exclusion_reason
    from pharmacy_exclusions

)

, combined_exclusions as (

    select
        exclusion_patients.person_id
      , exclusion_patients.exclusion_date
      , exclusion_patients.exclusion_reason
    from exclusion_patients
    inner join denominator
      on exclusion_patients.person_id = denominator.person_id

)

, add_data_types as (

    select
        distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(1 as integer) as exclusion_flag
    from combined_exclusions

)

select
      person_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
