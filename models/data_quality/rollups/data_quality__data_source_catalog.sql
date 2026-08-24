{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'data_source_catalog',
     tags = ['data_quality', 'dq_rollup'],
     materialized = 'table'
   )
}}

with source_tables as (

    select distinct
          populations.data_source
        , catalog.input_table_type as data_source_type
        , populations.table_name as input_table_name
        , cast(max(populations.row_count) as {{ dbt.type_bigint() }}) as row_count
    from {{ ref('data_quality__structural_source_populations') }} as populations
    inner join {{ ref('data_quality__input_layer_catalog') }} as catalog
        on populations.table_name = catalog.input_table_name
    group by
          populations.data_source
        , catalog.input_table_type
        , populations.table_name

)

select
      data_source
    , data_source_type
    , cast(count(*) as {{ dbt.type_int() }}) as input_table_count
    , cast(sum(coalesce(row_count, 0)) as {{ dbt.type_bigint() }}) as row_count
from source_tables
where coalesce(row_count, 0) > 0
group by
      data_source
    , data_source_type
