{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_missing_columns',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

select
      input_table_name
    , column_name
    , expected_data_type
from {{ ref('data_quality__structural_column_details') }}
where column_exists = 'no'
group by
      input_table_name
    , column_name
    , expected_data_type
