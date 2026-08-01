{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'data_source_catalog',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

with source_tables as (

    select distinct
          actual.data_source
        , catalog.input_table_type as data_source_type
        , actual.table_name as input_table_name
        , cast(max(actual.row_count) as {{ dbt.type_int() }}) as row_count
    from {{ ref('data_quality__structural_actual_columns') }} as actual
    inner join {{ ref('data_quality__input_layer_catalog') }} as catalog
        on actual.table_name = catalog.input_table_name
    group by
          actual.data_source
        , catalog.input_table_type
        , actual.table_name

)

select
      data_source
    , data_source_type
    , cast(count(*) as {{ dbt.type_int() }}) as input_table_count
    , cast(sum(coalesce(row_count, 0)) as {{ dbt.type_int() }}) as row_count
from source_tables
where coalesce(row_count, 0) > 0
group by
      data_source
    , data_source_type
