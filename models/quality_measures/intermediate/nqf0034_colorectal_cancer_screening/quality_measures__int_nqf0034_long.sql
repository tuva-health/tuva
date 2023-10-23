{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}


{%- set measure_id -%}
(
    select id
from {{ ref('quality_measures__measures') }}
where id = 'NQF0034'
    )
{%- endset -%}

{%- set measure_name -%}
(

    select name
from {{ ref('quality_measures__measures') }}
where id = 'NQF0034'

    )
{%- endset -%}

{%- set measure_version -%}
(
    select version
from {{ ref('quality_measures__measures') }}
where id = 'NQF0034'

    )
{%- endset -%}



/* selecting the full patient population as the grain of this table */
with patient as (

    select distinct patient_id
    from {{ ref('quality_measures__stg_core__patient') }}

)

, denominator as (

    select
          patient_id
    from {{ ref('quality_measures__int_nqf0034_denominator') }}

)

, numerator as (

    select
          patient_id
        , evidence_date
    from {{ ref('quality_measures__int_nqf0034_numerator') }}

)

, exclusions as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf0034_exclusions') }}

)

, measure_flags as (

    select
          patient.patient_id
        , case
            when denominator.patient_id is not null
            then 1
            else null
          end as denominator_flag
        , case
            when numerator.patient_id is not null
            then 1
            else null
          end as numerator_flag
        , case
            when exclusions.patient_id is not null
            then 1
            else null
          end as exclusion_flag
        , numerator.evidence_date
        , exclusions.exclusion_date
        , exclusions.exclusion_reason
        , pp.performance_period_begin
        , pp.performance_period_end
        , {{ measure_id }}  as measure_id
        , {{ measure_name }}  as measure_name
        , {{ measure_version }}  as measure_version
    from patient
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on 1 = 1
        left join denominator
            on patient.patient_id = denominator.patient_id
        left join numerator
            on patient.patient_id = numerator.patient_id
        left join exclusions
            on patient.patient_id = exclusions.patient_id

)

/*
    Deduplicate measure rows by latest evidence date or exclusion date
*/
, add_rownum as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , row_number() over(
            partition by
                  patient_id
                , performance_period_begin
                , performance_period_end
                , measure_id
                , measure_name
            order by
                  evidence_date desc nulls last
                , exclusion_date desc nulls last
          ) as row_num
    from measure_flags

)

, deduped as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from add_rownum
    where row_num = 1

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(evidence_date as date) as evidence_date
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
    from deduped

)

select
      patient_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , evidence_date
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types