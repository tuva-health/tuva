{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('claims_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'eligibility_span_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set date_type = api.Column.translate_type('date') %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_reasonable_date_sql = dq_date_literal_sql('1900-01-01') %}
{% set min_file_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set one_year_after_current_date_sql = "cast(" ~ dbt.dateadd(
    datepart='month',
    interval=12,
    from_date_or_timestamp=current_date_sql
  ) ~ " as " ~ date_type ~ ")" %}
{% set legacy_open_end_date_sql = dq_date_literal_sql('9999-12-31') %}
{% set death_flag_text_sql = "trim(cast(source_rows.death_flag as " ~ string_type ~ "))" %}
{% set open_end_where_sql =
    "(source_rows.enrollment_end_date is null"
    ~ " or source_rows.enrollment_end_date = " ~ legacy_open_end_date_sql ~ ")"
%}
{% set finite_end_where_sql =
    "source_rows.enrollment_end_date is not null"
    ~ " and source_rows.enrollment_end_date <> " ~ legacy_open_end_date_sql
%}
{% set valid_span_where_sql =
    "source_rows.enrollment_start_date is not null"
    ~ " and (" ~ open_end_where_sql
    ~ " or source_rows.enrollment_start_date <= source_rows.enrollment_end_date)"
%}
{% set complete_coverage_identity_where_sql =
    "source_rows.person_id is not null"
    ~ " and source_rows.member_id is not null"
    ~ " and source_rows.payer is not null"
    ~ " and source_rows." ~ quote_column('plan') ~ " is not null"
    ~ " and source_rows.data_source is not null"
%}

with source_rows as (
    select *
    from {{ ref('input_layer__eligibility') }}
),

coverage_windows as (
    select
          source_rows.*
        , max(case
            when source_rows.enrollment_start_date is not null
             and source_rows.enrollment_end_date is not null
             and source_rows.enrollment_end_date <> {{ legacy_open_end_date_sql }}
             and source_rows.enrollment_start_date <= source_rows.enrollment_end_date
                then source_rows.enrollment_end_date
          end) over (
            partition by
                  source_rows.person_id
                , source_rows.member_id
                , source_rows.payer
                , source_rows.{{ quote_column('plan') }}
                , source_rows.data_source
            order by
                  source_rows.enrollment_start_date
                , source_rows.enrollment_end_date
            rows between unbounded preceding and 1 preceding
          ) as _dq_max_prior_enrollment_end_date
        , sum(case
            when source_rows.enrollment_start_date is not null
             and (
                    source_rows.enrollment_end_date is null
                 or source_rows.enrollment_end_date = {{ legacy_open_end_date_sql }}
             )
                then 1
            else 0
          end) over (
            partition by
                  source_rows.person_id
                , source_rows.member_id
                , source_rows.payer
                , source_rows.{{ quote_column('plan') }}
                , source_rows.data_source
            order by
                  source_rows.enrollment_start_date
                , source_rows.enrollment_end_date
            rows between unbounded preceding and 1 preceding
          ) as _dq_prior_open_span_count
        , min(case
            when source_rows.enrollment_start_date is not null
             and (
                    source_rows.enrollment_end_date is null
                 or source_rows.enrollment_end_date = {{ legacy_open_end_date_sql }}
                 or source_rows.enrollment_start_date <= source_rows.enrollment_end_date
             )
                then source_rows.enrollment_start_date
          end) over (
            partition by
                  source_rows.person_id
                , source_rows.member_id
                , source_rows.payer
                , source_rows.{{ quote_column('plan') }}
                , source_rows.data_source
            order by
                  source_rows.enrollment_start_date
                , source_rows.enrollment_end_date
            rows between 1 following and unbounded following
          ) as _dq_min_following_enrollment_start_date
        , sum(case
            when source_rows.enrollment_start_date is not null
             and (
                    source_rows.enrollment_end_date is null
                 or source_rows.enrollment_end_date = {{ legacy_open_end_date_sql }}
             )
                then 1
            else 0
          end) over (
            partition by
                  source_rows.person_id
                , source_rows.member_id
                , source_rows.payer
                , source_rows.{{ quote_column('plan') }}
                , source_rows.data_source
          ) as _dq_open_span_count
    from source_rows
),

final as (
    select
          source_rows.person_id
        , source_rows.member_id
        , source_rows.enrollment_start_date
        , source_rows.payer
        , source_rows.{{ quote_column('plan') }}
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.sex is null", "1 = 1") }} as sex_null
        , {{ dq_logical_int_flag_sql(
            "cast(source_rows.sex as " ~ string_type ~ ") not in ('male', 'female', 'unknown')",
            "source_rows.sex is not null"
          ) }} as sex_invalid
        , {{ dq_logical_int_flag_sql("source_rows.race is null", "1 = 1") }} as race_null
        , {{ dq_logical_int_flag_sql(
            "race_lookup.description is null",
            "source_rows.race is not null"
          ) }} as race_invalid
        , {{ dq_logical_int_flag_sql("source_rows.birth_date is null", "1 = 1") }} as birth_date_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.birth_date > source_rows.death_date",
            "source_rows.birth_date is not null and source_rows.death_date is not null"
          ) }} as birth_date_after_death_date
        , {{ dq_logical_int_flag_sql(
            "source_rows.birth_date < " ~ min_reasonable_date_sql ~ " or source_rows.birth_date > " ~ current_date_sql,
            "source_rows.birth_date is not null"
          ) }} as birth_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.death_date < " ~ min_reasonable_date_sql ~ " or source_rows.death_date > " ~ current_date_sql,
            "source_rows.death_date is not null"
          ) }} as death_date_out_of_reasonable_range
        , {{ dq_logical_date_range_flag_sql(
            "source_rows.enrollment_start_date",
            min_reasonable_date_sql,
            one_year_after_current_date_sql
          ) }} as enrollment_start_date_outside_supported_date_range
        , {{ dq_logical_date_range_flag_sql(
            "source_rows.enrollment_end_date",
            min_reasonable_date_sql,
            one_year_after_current_date_sql,
            finite_end_where_sql
          ) }} as enrollment_end_date_outside_supported_date_range
        , {{ dq_logical_date_range_flag_sql(
            "source_rows.file_date",
            min_file_date_sql,
            current_date_sql
          ) }} as file_date_outside_supported_date_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
            "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_binary_value_flag_sql("source_rows.death_flag") }} as death_flag_invalid
        , {{ dq_logical_int_flag_sql(
            "source_rows.death_date is null",
            "source_rows.death_flag is not null and " ~ death_flag_text_sql ~ " = '1'"
          ) }} as death_flag_without_death_date
        , {{ dq_logical_binary_value_flag_sql("source_rows.hospice_flag") }} as hospice_flag_invalid
        , {{ dq_logical_binary_value_flag_sql("source_rows.institutional_snp_flag") }} as institutional_snp_flag_invalid
        , {{ dq_logical_binary_value_flag_sql("source_rows.long_term_institutional_flag") }} as long_term_institutional_flag_invalid
        , {{ dq_logical_int_flag_sql(
            "source_rows.enrollment_start_date > source_rows.enrollment_end_date",
            "source_rows.enrollment_start_date is not null and " ~ finite_end_where_sql
          ) }} as enrollment_start_after_end
        , {{ dq_logical_int_flag_sql(
            "coalesce(source_rows._dq_prior_open_span_count, 0) > 0"
            ~ " or source_rows._dq_max_prior_enrollment_end_date >= source_rows.enrollment_start_date"
            ~ " or (source_rows._dq_min_following_enrollment_start_date is not null and ("
            ~ open_end_where_sql
            ~ " or source_rows._dq_min_following_enrollment_start_date <= source_rows.enrollment_end_date))",
            complete_coverage_identity_where_sql
            ~ " and " ~ valid_span_where_sql
            ~ " and " ~ finite_end_where_sql
          ) }} as overlapping_enrollment_spans
        , {{ dq_logical_int_flag_sql(
            "source_rows._dq_open_span_count > 1",
            complete_coverage_identity_where_sql
            ~ " and source_rows.enrollment_start_date is not null"
            ~ " and " ~ open_end_where_sql
          ) }} as multiple_open_enrollment_spans
        , {{ dq_logical_int_flag_sql("source_rows.payer_type is null", "1 = 1") }} as payer_type_null
        , {{ dq_logical_int_flag_sql(
            "payer_type_lookup.payer_type is null",
            "source_rows.payer_type is not null"
          ) }} as payer_type_invalid
    from coverage_windows as source_rows
    left join {{ ref('terminology__race') }} as race_lookup
        on lower(cast(source_rows.race as {{ string_type }})) = lower(cast(race_lookup.description as {{ string_type }}))
    left join {{ ref('terminology__payer_type') }} as payer_type_lookup
        on lower(cast(source_rows.payer_type as {{ string_type }})) = lower(cast(payer_type_lookup.payer_type as {{ string_type }}))
)

select *
from final
