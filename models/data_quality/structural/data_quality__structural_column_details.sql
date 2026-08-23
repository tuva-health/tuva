{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_column_details',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

with expected as (

    select *
    from {{ ref('data_quality__structural_expected_columns') }}

)

, actual_columns as (

    select
          input_layer_domain
        , table_name
        , model_name
        , column_name
        , actual_column_name
        , actual_data_type
        , actual_type_family
    from {{ ref('data_quality__structural_actual_columns') }}
    where column_name is not null

)

select
      expected.table_name as input_table_name
    , expected.column_name
    , expected.expected_data_type
    , actual_columns.actual_column_name
    , actual_columns.actual_data_type
    , expected.is_primary_key
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
left outer join actual_columns
    on expected.input_layer_domain = actual_columns.input_layer_domain
    and expected.table_name = actual_columns.table_name
    and expected.model_name = actual_columns.model_name
    and expected.column_name = actual_columns.column_name
