{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}

with normalized as (

    select *
    from {{ ref('quality_measures__int_hedis_measure_normalize') }}

)

, measure_name as (

    select *
    from {{ ref('quality_measures__measures') }}

)

/* add clean measure name */
, add_data_types as (

    select
          cast(patient as {{ dbt.type_string() }}) as person_id
        , cast(rate_1_denominator_flag as integer) as rate_1_denominator_flag
        , cast(rate_1_numerator_flag as integer) as rate_1_numerator_flag
        , cast(rate_1_exclusion_flag as integer) as rate_1_exclusion_flag
        , cast(rate_1_medicare_denominator_flag as integer) as rate_1_medicare_denominator_flag
        , cast(rate_1_medicare_exclusion_flag as integer) as rate_1_medicare_exclusion_flag
        , cast(rate_1_performance_flag as integer) as rate_1_performance_flag
        , cast(rate_1_medicare_performance_flag as integer) as rate_1_medicare_performance_flag
        , cast(rate_2_denominator_flag as integer) as rate_2_denominator_flag
        , cast(rate_2_numerator_flag as integer) as rate_2_numerator_flag
        , cast(rate_2_exclusion_flag as integer) as rate_2_exclusion_flag
        , cast(rate_2_medicare_denominator_flag as integer) as rate_2_medicare_denominator_flag
        , cast(rate_2_medicare_exclusion_flag as integer) as rate_2_medicare_exclusion_flag
        , cast(rate_2_performance_flag as integer) as rate_2_performance_flag
        , cast(rate_2_medicare_performance_flag as integer) as rate_2_medicare_performance_flag
        , cast(period_start as date) as performance_period_begin
        , cast(period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name.name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_year as {{ dbt.type_string() }}) as measure_version
        , cast(data_source as {{ dbt.type_string() }}) as data_source
    from normalized
        left outer join measure_name
            on normalized.measure_id = measure_name.id
            and normalized.measure_year = measure_name.version

)

select
      person_id
    , rate_1_denominator_flag
    , rate_1_numerator_flag
    , rate_1_exclusion_flag
    , rate_1_medicare_denominator_flag
    , rate_1_medicare_exclusion_flag
    , rate_1_performance_flag
    , rate_1_medicare_performance_flag
    , rate_2_denominator_flag
    , rate_2_numerator_flag
    , rate_2_exclusion_flag
    , rate_2_medicare_denominator_flag
    , rate_2_medicare_exclusion_flag
    , rate_2_performance_flag
    , rate_2_medicare_performance_flag
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types