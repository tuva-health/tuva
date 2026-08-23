{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_data_type_mismatches',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

select
      column_details.input_table_name
    , column_details.column_name
    , column_details.expected_data_type
    , column_details.actual_data_type
from {{ ref('data_quality__structural_column_details') }} as column_details
where column_details.column_exists = 'yes'
  and column_details.data_type_correct = 'no'
group by
      column_details.input_table_name
    , column_details.column_name
    , column_details.expected_data_type
    , column_details.actual_data_type
