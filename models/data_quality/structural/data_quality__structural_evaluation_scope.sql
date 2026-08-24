{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_evaluation_scope',
     tags = ['data_quality', 'dq_structural'],
     materialized = 'table'
   )
}}

with enabled_models as (

    select distinct
          input_layer_domain
        , table_name as input_table_name
        , model_name
    from {{ ref('data_quality__structural_expected_columns') }}

)

, actual_source_populations as (

    select
          input_layer_domain
        , table_name as input_table_name
        , model_name
        , data_source
        , data_source_key
        , cast(max(row_count) as {{ dbt.type_bigint() }}) as row_count
    from {{ ref('data_quality__structural_source_populations') }}
    group by
          input_layer_domain
        , table_name
        , model_name
        , data_source
        , data_source_key

)

, domain_grid as (

    select
          enabled_models.input_layer_domain
        , enabled_models.input_table_name
        , enabled_models.model_name
        , data_sources.data_source
        , data_sources.data_source_key
        , cast(coalesce(actual_source_populations.row_count, 0) as {{ dbt.type_bigint() }}) as row_count
    from enabled_models
    inner join {{ ref('data_quality__structural_data_sources') }} as data_sources
        on enabled_models.input_layer_domain = data_sources.input_layer_domain
    left outer join actual_source_populations
        on enabled_models.input_layer_domain = actual_source_populations.input_layer_domain
        and enabled_models.input_table_name = actual_source_populations.input_table_name
        and enabled_models.model_name = actual_source_populations.model_name
        and data_sources.data_source_key = actual_source_populations.data_source_key

)

, model_specific_null_sources as (

    select
          actual_source_populations.input_layer_domain
        , actual_source_populations.input_table_name
        , actual_source_populations.model_name
        , actual_source_populations.data_source
        , actual_source_populations.data_source_key
        , actual_source_populations.row_count
    from actual_source_populations
    where actual_source_populations.data_source is null
      and actual_source_populations.row_count > 0
      and not exists (
          select 1
          from domain_grid
          where domain_grid.input_layer_domain = actual_source_populations.input_layer_domain
            and domain_grid.input_table_name = actual_source_populations.input_table_name
            and domain_grid.model_name = actual_source_populations.model_name
            and domain_grid.data_source_key = actual_source_populations.data_source_key
      )

)

select *
from domain_grid

union all

select *
from model_specific_null_sources
