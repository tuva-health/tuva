{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'patient_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_birth_death_date_sql = dq_date_literal_sql('1900-01-01') %}
{% set zip_raw_expression = "trim(cast(source_rows.zip_code as " ~ string_type ~ "))" %}
{% set zip_compact_expression = "replace(" ~ zip_raw_expression ~ ", '-', '')" %}
{% set zip_raw_is_digits_expression = dq_digits_only_sql(zip_raw_expression) %}
{% set zip_compact_is_digits_expression = dq_digits_only_sql(zip_compact_expression) %}
{% set zip_valid_expression = "("
    ~ "(" ~ length(zip_raw_expression) ~ " = 5 and " ~ zip_raw_is_digits_expression ~ ")"
    ~ " or (" ~ length(zip_raw_expression) ~ " = 9 and " ~ zip_raw_is_digits_expression ~ ")"
    ~ " or (" ~ length(zip_raw_expression) ~ " = 10 and " ~ substring(zip_raw_expression, 6, 1) ~ " = '-'"
    ~ " and " ~ length(zip_compact_expression) ~ " = 9 and " ~ zip_compact_is_digits_expression ~ ")"
    ~ ")"
%}

with source_rows as (
    select *
    from {{ ref('input_layer__patient') }}
),

state_rows as (
    select distinct
          ansi_fips_state_code
        , ansi_fips_state_abbreviation
        , ansi_fips_state_name
    from {{ ref('terminology__ansi_fips_state') }}
),

person_level_flags as (
    select
          person_id
        , data_source
        , {{ dq_logical_int_flag_sql(
              "count(distinct case when sex is not null then cast(sex as " ~ string_type ~ ") end) > 1",
              "count(*) > 1 and count(case when sex is not null then 1 end) > 0"
          ) }} as multiple_sexes_per_person
        , {{ dq_logical_int_flag_sql(
              "count(distinct birth_date) > 1",
              "count(*) > 1 and count(case when birth_date is not null then 1 end) > 0"
          ) }} as multiple_birth_dates_per_person
    from source_rows
    group by
          person_id
        , data_source
),

final as (
    select
          source_rows.person_id
        , source_rows.patient_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.sex is null", "1 = 1") }} as sex_null
        , {{ dq_logical_int_flag_sql(
              "cast(source_rows.sex as " ~ string_type ~ ") not in ('male', 'female', 'unknown')",
              "source_rows.sex is not null"
          ) }} as sex_invalid
        , {{ dq_logical_int_flag_sql("race_lookup.description is null", "source_rows.race is not null") }} as race_invalid
        , {{ dq_logical_int_flag_sql("ethnicity_lookup.code is null", "source_rows.ethnicity is not null") }} as ethnicity_invalid
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is null", "1 = 1") }} as birth_date_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.birth_date < " ~ min_birth_death_date_sql ~ " or source_rows.birth_date > " ~ current_date_sql,
              "source_rows.birth_date is not null"
          ) }} as birth_date_out_of_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.death_date < " ~ min_birth_death_date_sql ~ " or source_rows.death_date > " ~ current_date_sql,
              "source_rows.death_date is not null"
          ) }} as death_date_out_of_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.birth_date > source_rows.death_date",
              "source_rows.birth_date is not null and source_rows.death_date is not null"
          ) }} as birth_date_after_death_date
        , {{ dq_logical_int_flag_sql(
              "source_rows.death_flag not in (0, 1)",
              "source_rows.death_flag is not null"
          ) }} as death_flag_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.death_date is null",
              "source_rows.death_flag = 1"
          ) }} as death_flag_without_death_date
        , {{ dq_logical_int_flag_sql(
              "source_rows.death_flag is null or source_rows.death_flag = 0",
              "source_rows.death_date is not null and (source_rows.death_flag is null or source_rows.death_flag in (0, 1))"
          ) }} as death_date_without_death_flag
        , {{ dq_logical_int_flag_sql(
              "source_rows.state is not null "
              ~ "and state_rows.ansi_fips_state_code is null "
              ~ "and state_rows.ansi_fips_state_abbreviation is null "
              ~ "and state_rows.ansi_fips_state_name is null"
          , "source_rows.state is not null"
          ) }} as state_invalid
        , {{ dq_logical_int_flag_sql("not " ~ zip_valid_expression, "source_rows.zip_code is not null") }} as zip_code_invalid_format
        , person_level_flags.multiple_sexes_per_person
        , person_level_flags.multiple_birth_dates_per_person
    from source_rows
    left join {{ ref('terminology__race') }} as race_lookup
        on lower(cast(source_rows.race as {{ string_type }})) = lower(cast(race_lookup.description as {{ string_type }}))
    left join {{ ref('terminology__ethnicity') }} as ethnicity_lookup
        on lower(cast(source_rows.ethnicity as {{ string_type }})) = lower(cast(ethnicity_lookup.code as {{ string_type }}))
    left join state_rows
        on lower(cast(source_rows.state as {{ string_type }})) = lower(cast(state_rows.ansi_fips_state_code as {{ string_type }}))
        or lower(cast(source_rows.state as {{ string_type }})) = lower(cast(state_rows.ansi_fips_state_abbreviation as {{ string_type }}))
        or lower(cast(source_rows.state as {{ string_type }})) = lower(cast(state_rows.ansi_fips_state_name as {{ string_type }}))
    left join person_level_flags
        on source_rows.person_id = person_level_flags.person_id
       and source_rows.data_source = person_level_flags.data_source
)

select *
from final
