{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'patient_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_birth_death_date_sql = dq_date_literal_sql('1900-01-01') %}
{% set zip_raw_expression = "trim(cast(source_rows.zip_code as " ~ string_type ~ "))" %}
{% set zip_compact_expression = "replace(" ~ zip_raw_expression ~ ", '-', '')" %}
{% set zip_valid_expression = "("
    ~ "(" ~ length(zip_raw_expression) ~ " = 5 and " ~ try_to_cast_int(zip_raw_expression) ~ " is not null)"
    ~ " or (" ~ length(zip_raw_expression) ~ " = 9 and " ~ try_to_cast_int(zip_raw_expression) ~ " is not null)"
    ~ " or (" ~ length(zip_raw_expression) ~ " = 10 and " ~ substring(zip_raw_expression, 6, 1) ~ " = '-' and " ~ try_to_cast_int(zip_compact_expression) ~ " is not null)"
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
        , {{ dq_logical_int_flag_sql("count(distinct case when sex is not null then lower(cast(sex as " ~ string_type ~ ")) end) > 1") }} as multiple_sexes_per_person
        , {{ dq_logical_int_flag_sql("count(distinct birth_date) > 1") }} as multiple_birth_dates_per_person
    from source_rows
    group by
          person_id
        , data_source
),

final as (
    select
          source_rows.person_id
        , source_rows.patient_id
        , source_rows.sex
        , source_rows.race
        , source_rows.ethnicity
        , source_rows.birth_date
        , source_rows.death_date
        , source_rows.death_flag
        , source_rows.state
        , source_rows.zip_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.sex is null") }} as sex_null
        , {{ dq_logical_int_flag_sql("source_rows.sex is not null and lower(cast(source_rows.sex as " ~ string_type ~ ")) not in ('male', 'female', 'unknown')") }} as sex_invalid
        , {{ dq_logical_int_flag_sql("source_rows.race is not null and race_lookup.description is null") }} as race_invalid
        , {{ dq_logical_int_flag_sql("source_rows.ethnicity is not null and ethnicity_lookup.code is null") }} as ethnicity_invalid
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is null") }} as birth_date_null
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is not null and (source_rows.birth_date < " ~ min_birth_death_date_sql ~ " or source_rows.birth_date > " ~ current_date_sql ~ ")") }} as birth_date_out_of_range
        , {{ dq_logical_int_flag_sql("source_rows.death_date is not null and (source_rows.death_date < " ~ min_birth_death_date_sql ~ " or source_rows.death_date > " ~ current_date_sql ~ ")") }} as death_date_out_of_range
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is not null and source_rows.death_date is not null and source_rows.birth_date > source_rows.death_date") }} as birth_date_after_death_date
        , {{ dq_logical_int_flag_sql("source_rows.death_flag is not null and lower(cast(source_rows.death_flag as " ~ string_type ~ ")) not in ('0', '1')") }} as death_flag_invalid
        , {{ dq_logical_int_flag_sql("source_rows.death_flag is not null and lower(cast(source_rows.death_flag as " ~ string_type ~ ")) = '1' and source_rows.death_date is null") }} as death_flag_without_death_date
        , {{ dq_logical_int_flag_sql("source_rows.death_date is not null and (source_rows.death_flag is null or lower(cast(source_rows.death_flag as " ~ string_type ~ ")) = '0')") }} as death_date_without_death_flag
        , {{ dq_logical_int_flag_sql(
              "source_rows.state is not null "
              ~ "and state_rows.ansi_fips_state_code is null "
              ~ "and state_rows.ansi_fips_state_abbreviation is null "
              ~ "and state_rows.ansi_fips_state_name is null"
          ) }} as state_invalid
        , {{ dq_logical_int_flag_sql("source_rows.zip_code is not null and not " ~ zip_valid_expression) }} as zip_code_invalid_format
        , coalesce(person_level_flags.multiple_sexes_per_person, cast(0 as {{ dbt.type_int() }})) as multiple_sexes_per_person
        , coalesce(person_level_flags.multiple_birth_dates_per_person, cast(0 as {{ dbt.type_int() }})) as multiple_birth_dates_per_person
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
