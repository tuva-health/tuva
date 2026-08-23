{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_primary_key_failure_counts',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

select
      primary_key_tests.data_source
    , primary_key_tests.input_table_name
    , case
        when primary_key_tests.test = 'not null' then 'null_value'
        when primary_key_tests.test = 'duplicate value' then 'duplicate_value'
      end as failure_type
    , primary_key_tests.column_name as primary_key_columns
    , cast(primary_key_tests.test_result as {{ dbt.type_bigint() }}) as failed_record_count
from {{ ref('data_quality__structural_primary_key_tests') }} as primary_key_tests
inner join {{ ref('data_quality__structural') }} as structural
    on primary_key_tests.input_table_name = structural.input_table_name
    and (
        primary_key_tests.data_source = structural.data_source
        or (
            primary_key_tests.data_source is null
            and structural.data_source is null
        )
    )
where structural.primary_key_correct = 'fail'
  and primary_key_tests.test_result > 0
  and primary_key_tests.test in ('not null', 'duplicate value')
