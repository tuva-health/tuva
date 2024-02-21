{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with exclusions as (

select *
from {{ref('quality_measures__int_nqf0059_exclude_advanced_illness')}}

union all

select *
from {{ref('quality_measures__int_nqf0059_exclude_dementia')}}

union all

select *
from {{ref('quality_measures__int_nqf0059_exclude_hospice_palliative')}}

union all

select *
from {{ref('quality_measures__int_nqf0059_exclude_institutional_snp')}}

)

, valid_exclusions as (

  select 
    exclusions.*
  from exclusions
  inner join {{ref('quality_measures__int_nqf0059_denominator')}} p
      on exclusions.patient_id = p.patient_id

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
