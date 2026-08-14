{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('claims_enabled', false) | as_bool)
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('stg_input_layer__eligibility') }}
),

final as (
    select
          source_rows.person_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("count(distinct case when source_rows.sex is not null then lower(cast(source_rows.sex as " ~ string_type ~ ")) end) > 1") }} as multiple_sexes_per_person
        , {{ dq_logical_int_flag_sql("count(distinct case when source_rows.race is not null then lower(cast(source_rows.race as " ~ string_type ~ ")) end) > 1") }} as multiple_races_per_person
        , {{ dq_logical_int_flag_sql("count(distinct case when source_rows.birth_date is not null then source_rows.birth_date end) > 1") }} as multiple_birth_dates_per_person
    from source_rows
    group by
          source_rows.person_id
        , source_rows.data_source
)

select *
from final
