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

with non_null_sources as (

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
      source_populations.input_layer_domain
    , cast(null as {{ dbt.type_string() }}) as data_source
    , '{{ dq_structural_null_source_key() }}' as data_source_key
from {{ ref('data_quality__structural_source_populations') }} as source_populations
group by source_populations.input_layer_domain
having count(source_populations.data_source) = 0
