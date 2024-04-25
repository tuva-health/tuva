{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_nqf0034__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_nqf0034__performance_period') }}

)
{%- endset -%}

with frailty as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
  from {{ ref('quality_measures__int_shared_exclusions_frailty') }}
  where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, denominator as (

  select
      patient_id
    , max_age as age
  from {{ref('quality_measures__int_nqf0034_denominator')}}

)
-- advanced illness start
, advanced_illness_exclusion as (

  select
    source.*
  from {{ ref('quality_measures__int_shared_exclusions_advanced_illness') }} as source
  inner join frailty
    on source.patient_id = frailty.patient_id
  where source.exclusion_date
    between
      {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp=performance_period_begin) }}
      and {{ performance_period_end }}

)

, acute_inpatient_advanced_illness as (

  select
    *
  from advanced_illness_exclusion
  where patient_type = 'acute_inpatient'

)

, nonacute_outpatient_advanced_illness as (

  select
    *
  from advanced_illness_exclusion
  where patient_type = 'nonacute_outpatient'

)

, acute_inpatient_counts as (

    select
          patient_id
        , exclusion_type
        , count(distinct exclusion_date) as encounter_count
    from acute_inpatient_advanced_illness
    group by patient_id, exclusion_type

)

, nonacute_outpatient_counts as (

    select
          patient_id
        , exclusion_type
        , count(distinct exclusion_date) as encounter_count
    from nonacute_outpatient_advanced_illness
    group by patient_id, exclusion_type

)

, valid_advanced_illness_exclusions as (

    select
          acute_inpatient_advanced_illness.patient_id
        , acute_inpatient_advanced_illness.exclusion_date
        , acute_inpatient_advanced_illness.exclusion_reason
        , acute_inpatient_advanced_illness.exclusion_type
    from acute_inpatient_advanced_illness
    left join acute_inpatient_counts
      on acute_inpatient_advanced_illness.patient_id = acute_inpatient_counts.patient_id
    where acute_inpatient_counts.encounter_count >= 1

    union all

    select
        nonacute_outpatient_advanced_illness.patient_id
      , nonacute_outpatient_advanced_illness.exclusion_date
      , nonacute_outpatient_advanced_illness.exclusion_reason
      , nonacute_outpatient_advanced_illness.exclusion_type
    from nonacute_outpatient_advanced_illness
    left join nonacute_outpatient_counts
      on nonacute_outpatient_advanced_illness.patient_id = nonacute_outpatient_counts.patient_id
    where nonacute_outpatient_counts.encounter_count >= 2


)
-- advanced illness end

, valid_dementia_exclusions as (

  select
      source.patient_id
    , source.exclusion_date
    , source.exclusion_reason
    , source.exclusion_type
  from {{ref('quality_measures__int_shared_exclusions_dementia')}} source
  inner join frailty
    on source.patient_id = frailty.patient_id
  where (
    source.dispensing_date
      between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp= performance_period_begin ) }}
          and {{ performance_period_end }}
    or source.paid_date
      between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp= performance_period_begin ) }}
          and {{ performance_period_end }}
    )

)

, valid_hospice_palliative as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from {{ref('quality_measures__int_shared_exclusions_hospice_palliative')}}
  where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}
    and lower(exclusion_reason) in 
  (
        'palliative care encounter'
      , 'palliative care intervention'
      , 'hospice care ambulatory'
      , 'hospice encounter'
  )

)

, valid_institutional_snp as (

  select 
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from {{ref('quality_measures__int_shared_exclusions_institutional_snp')}}
  where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, measure_specific_colectomy_colorectal_cancer_exclusion as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from {{ref('quality_measures__int_nqf0034_exclude_colectomy_cancer')}}

)

, exclusions as (

    select *
    from valid_advanced_illness_exclusions
  
    union all

    select *
    from valid_dementia_exclusions

    union all

    select *
    from valid_hospice_palliative

    union all

    select *
    from valid_institutional_snp

    union all

    select *
    from valid_dementia_exclusions

    union all

    select *
    from measure_specific_colectomy_colorectal_cancer_exclusion

)

, combined_exclusions as (

  select 
      exclusions.*
    , denominator.age
  from exclusions
  inner join denominator
      on exclusions.patient_id = denominator.patient_id

)

, valid_exclusions as (

  select * from combined_exclusions
  where exclusion_type not in (
      'measure specific exclusion for historical record of colectomy cancer'
    , 'hospice_palliative'
    ) 
    and age >= 66

  union all

  select * from combined_exclusions --age irrelvant exclusions
  where exclusion_type in (
      'measure specific exclusion for historical record of colectomy cancer'
    , 'hospice_palliative'
    ) 

)

, add_data_types as (

    select
        distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , 1 as exclusion_flag
    from valid_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types