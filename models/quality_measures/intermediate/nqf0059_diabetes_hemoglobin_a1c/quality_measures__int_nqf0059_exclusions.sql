{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with exclusions as (

select *
  , 'advanced_illness' as exclusion_type
from {{ref('quality_measures__int_nqf0059_exclude_advanced_illness')}}

union all

select *
  , 'dementia' as exclusion_type
from {{ref('quality_measures__int_nqf0059_exclude_dementia')}}

union all

select *
from {{ref('shared_exclusions__exclude_hospice_palliative')}}

union all

select *
from {{ref('shared_exclusions__exclude_institutional_snp')}}

)

, combined_exclusions as (

  select 
      exclusions.*
    , denominator.age
  from exclusions
  inner join {{ref('quality_measures__int_nqf0059_denominator')}} as denominator
      on exclusions.patient_id = denominator.patient_id
  where exclusions.exclusion_date between denominator.performance_period_begin and denominator.performance_period_end

)

, valid_exclusions as (

  select
    *
  from combined_exclusions
  where exclusion_type != 'hospice_palliative'
    and age >= 66

  union all

  select
    *
  from combined_exclusions
  where exclusion_type = 'hospice_palliative'

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , tuva_last_run
        , 1 as exclusion_flag
    from valid_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , tuva_last_run
from add_data_types
