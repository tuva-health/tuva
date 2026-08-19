{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'location_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
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
    from {{ ref('input_layer__location') }}
),

provider_rows as (
    select distinct
          npi
    from {{ ref('provider_data__provider') }}
),

state_rows as (
    select distinct
          ansi_fips_state_code
        , ansi_fips_state_abbreviation
        , ansi_fips_state_name
    from {{ ref('terminology__ansi_fips_state') }}
),

final as (
    select
          source_rows.location_id
        , source_rows.npi
        , source_rows.state
        , source_rows.zip_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.npi is not null and provider_rows.npi is null") }} as npi_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.state is not null "
              ~ "and state_rows.ansi_fips_state_code is null "
              ~ "and state_rows.ansi_fips_state_abbreviation is null "
              ~ "and state_rows.ansi_fips_state_name is null"
          ) }} as state_invalid
        , {{ dq_logical_int_flag_sql("source_rows.zip_code is not null and not " ~ zip_valid_expression) }} as zip_code_invalid_format
    from source_rows
    left join provider_rows
        on cast(source_rows.npi as {{ string_type }}) = cast(provider_rows.npi as {{ string_type }})
    left join state_rows
        on lower(cast(source_rows.state as {{ string_type }})) = lower(cast(state_rows.ansi_fips_state_code as {{ string_type }}))
        or lower(cast(source_rows.state as {{ string_type }})) = lower(cast(state_rows.ansi_fips_state_abbreviation as {{ string_type }}))
        or lower(cast(source_rows.state as {{ string_type }})) = lower(cast(state_rows.ansi_fips_state_name as {{ string_type }}))
)

select *
from final
