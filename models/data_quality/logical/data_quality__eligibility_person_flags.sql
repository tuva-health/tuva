{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('claims_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'eligibility_person_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('input_layer__eligibility') }}
),

final as (
    select
          source_rows.person_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("count(distinct case when source_rows.gender is not null then lower(cast(source_rows.gender as " ~ string_type ~ ")) end) > 1") }} as multiple_genders_per_person
        , {{ dq_logical_int_flag_sql("count(distinct case when source_rows.race is not null then lower(cast(source_rows.race as " ~ string_type ~ ")) end) > 1") }} as multiple_races_per_person
        , {{ dq_logical_int_flag_sql("count(distinct case when source_rows.birth_date is not null then source_rows.birth_date end) > 1") }} as multiple_birth_dates_per_person
    from source_rows
    group by 1, 2
)

select *
from final
