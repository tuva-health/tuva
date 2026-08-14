{{ config(
     enabled = var('enable_data_quality', false) | as_bool
   )
}}

with expected as (

    select *
    from {{ ref('int_data_quality__structural_expected_column') }}

)

, actual_sources as (

    select
          table_name
        , model_name
        , data_source
        , data_source_key
        , max(table_exists) as table_exists
        , max(row_count) as row_count
    from {{ ref('int_data_quality__structural_actual_column') }}
    group by
          table_name
        , model_name
        , data_source
        , data_source_key

)

, actual_columns as (

    select *
    from {{ ref('int_data_quality__structural_actual_column') }}
    where column_name is not null

)

select
      actual_sources.data_source
    , expected.table_name as {{ adapter.quote('table') }}
    , expected.column_name as {{ adapter.quote('column') }}
    , expected.expected_data_type
    , actual_columns.actual_data_type
    , expected.is_primary_key
    , actual_sources.table_exists
    , cast(actual_sources.row_count as {{ dbt.type_int() }}) as row_count
    , case
        when actual_columns.column_name is not null then 'yes'
        else 'no'
      end as column_exists
    , case
        when actual_columns.column_name is null then 'no'
        when expected.expected_type_family is null then 'no'
        when actual_columns.actual_type_family is null then 'no'
        when {{ dq_type_families_match_sql('expected.expected_type_family', 'actual_columns.actual_type_family') }} then 'yes'
        else 'no'
      end as data_type_correct
    , expected.column_order
from expected
inner join actual_sources
    on expected.table_name = actual_sources.table_name
    and expected.model_name = actual_sources.model_name
left outer join actual_columns
    on expected.table_name = actual_columns.table_name
    and expected.model_name = actual_columns.model_name
    and expected.column_name = actual_columns.column_name
    and actual_sources.data_source_key = actual_columns.data_source_key
