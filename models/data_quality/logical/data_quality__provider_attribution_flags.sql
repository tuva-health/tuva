{{ config(
     enabled = (
       (var('data_quality_enabled', false) | as_bool)
       and (var('claims_enabled', false) | as_bool)
       and (var('provider_attribution_enabled', false) | as_bool)
     ),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'provider_attribution_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set year_month_expression = "cast(source_rows.year_month as " ~ dbt.type_string() ~ ")" %}
{% set trimmed_year_month_expression = "trim(" ~ year_month_expression ~ ")" %}
{% set sentinel_wrapped_year_month_expression = concat_custom([
    "'|'",
    year_month_expression,
    "'|'"
]) %}
{% set sentinel_wrapped_trimmed_year_month_expression = concat_custom([
    "'|'",
    trimmed_year_month_expression,
    "'|'"
]) %}
{% set digit_values = "('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')" %}
{% set month_values = "('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12')" %}
{% set valid_year_month_expression = "("
    ~ sentinel_wrapped_year_month_expression ~ " = " ~ sentinel_wrapped_trimmed_year_month_expression
    ~ " and " ~ length(trimmed_year_month_expression) ~ " = 6"
    ~ " and " ~ substring(trimmed_year_month_expression, 1, 1) ~ " in " ~ digit_values
    ~ " and " ~ substring(trimmed_year_month_expression, 2, 1) ~ " in " ~ digit_values
    ~ " and " ~ substring(trimmed_year_month_expression, 3, 1) ~ " in " ~ digit_values
    ~ " and " ~ substring(trimmed_year_month_expression, 4, 1) ~ " in " ~ digit_values
    ~ " and " ~ substring(trimmed_year_month_expression, 5, 2) ~ " in " ~ month_values
    ~ ")"
%}

with source_rows as (
    select *
    from {{ ref('input_layer__provider_attribution') }}
),

final as (
    select
          source_rows.person_id
        , source_rows.member_id
        , source_rows.year_month
        , source_rows.payer
        , source_rows.{{ quote_column('plan') }}
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql(
              "not " ~ valid_year_month_expression,
              "source_rows.year_month is not null"
          ) }} as year_month_invalid_format
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
    from source_rows
)

select *
from final
