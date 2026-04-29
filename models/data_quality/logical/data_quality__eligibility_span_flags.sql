{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('claims_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'eligibility_span_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_reasonable_date_sql = dq_date_literal_sql('1900-01-01') %}

with source_rows as (
    select *
    from {{ ref('input_layer__eligibility') }}
),

final as (
    select
          source_rows.person_id
        , source_rows.member_id
        , source_rows.enrollment_start_date
        , source_rows.enrollment_end_date
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.gender is null") }} as gender_null
        , {{ dq_logical_int_flag_sql("source_rows.gender is not null and lower(cast(source_rows.gender as " ~ string_type ~ ")) not in ('male', 'female', 'unknown')") }} as gender_invalid
        , {{ dq_logical_int_flag_sql("source_rows.race is null") }} as race_null
        , {{ dq_logical_int_flag_sql("source_rows.race is not null and race_lookup.description is null") }} as race_invalid
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is null") }} as birth_date_null
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is not null and source_rows.death_date is not null and source_rows.birth_date > source_rows.death_date") }} as birth_date_after_death_date
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is not null and (source_rows.birth_date < " ~ min_reasonable_date_sql ~ " or source_rows.birth_date > " ~ current_date_sql ~ ")") }} as birth_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.death_date is not null and (source_rows.death_date < " ~ min_reasonable_date_sql ~ " or source_rows.death_date > " ~ current_date_sql ~ ")") }} as death_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.death_flag is not null and lower(cast(source_rows.death_flag as " ~ string_type ~ ")) not in ('0', '1')") }} as death_flag_invalid
        , {{ dq_logical_int_flag_sql("source_rows.death_flag is not null and lower(cast(source_rows.death_flag as " ~ string_type ~ ")) = '1' and source_rows.death_date is null") }} as death_flag_without_death_date
        , {{ dq_logical_int_flag_sql("source_rows.enrollment_start_date is not null and source_rows.enrollment_end_date is not null and source_rows.enrollment_start_date > source_rows.enrollment_end_date") }} as enrollment_start_after_end
        , {{ dq_logical_int_flag_sql("source_rows.payer_type is null") }} as payer_type_null
        , {{ dq_logical_int_flag_sql("source_rows.payer_type is not null and payer_type_lookup.payer_type is null") }} as payer_type_invalid
    from source_rows
    left join {{ ref('terminology__race') }} as race_lookup
        on lower(cast(source_rows.race as {{ string_type }})) = lower(cast(race_lookup.description as {{ string_type }}))
    left join {{ ref('terminology__payer_type') }} as payer_type_lookup
        on lower(cast(source_rows.payer_type as {{ string_type }})) = lower(cast(payer_type_lookup.payer_type as {{ string_type }}))
)

select *
from final
