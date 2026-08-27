{{ config(
     enabled = the_tuva_project.tuva_boolean_var('data_quality_enabled', false),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_data_sources',
     tags = ['data_quality', 'dq_structural'],
     materialized = 'table'
   )
}}

with enabled_domains as (

    select distinct
          input_layer_domain
    from {{ ref('data_quality__structural_source_populations') }}

)

, non_null_sources as (

    select
          input_layer_domain
        , data_source
        , data_source_key
    from {{ ref('data_quality__structural_source_populations') }}
    where data_source is not null
    group by
          input_layer_domain
        , data_source
        , data_source_key

)

select
      input_layer_domain
    , data_source
    , data_source_key
from non_null_sources

union all

select
      enabled_domains.input_layer_domain
    , cast(null as {{ dbt.type_string() }}) as data_source
    , '{{ dq_structural_null_source_key() }}' as data_source_key
from enabled_domains
where not exists (
    select 1
    from non_null_sources
    where non_null_sources.input_layer_domain = enabled_domains.input_layer_domain
)
