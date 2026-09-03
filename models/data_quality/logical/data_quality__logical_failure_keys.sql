{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and (the_tuva_project.tuva_boolean_var('enable_data_quality_failure_keys', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical_failure_keys',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{#
    The manifest is emitted by data_quality__logical_failure_keys_part_N, which
    are implementation details rather than consumer contracts. Splitting it
    keeps each generated statement inside Athena's 262,144-byte query-string
    limit; this relation stays the stable public interface and is a thin union
    over the parts. dq_logical_chunk_count sets how many parts there are.
#}

{% set part_models = [] %}
{% for chunk_index in range(the_tuva_project.dq_logical_chunk_count()) %}
    {% do part_models.append(ref('data_quality__logical_failure_keys_part_' ~ (chunk_index + 1))) %}
{% endfor %}

select
      cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(input_table_name as {{ dbt.type_string() }}) as input_table_name
    , cast(test_name as {{ dbt.type_string() }}) as test_name
    , cast(grain as {{ dbt.type_string() }}) as grain
    , cast(key_columns as {{ dbt.type_string() }}) as key_columns
    , cast(key_values_format as {{ dbt.type_string() }}) as key_values_format
    , cast(key_values as {{ dbt.type_string() }}) as key_values
from (
    {% for part_model in part_models %}
    select
          data_source
        , input_table_name
        , test_name
        , grain
        , key_columns
        , key_values_format
        , key_values
    from {{ part_model }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
) as logical_failure_keys
