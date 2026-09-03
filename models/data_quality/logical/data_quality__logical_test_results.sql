{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical_test_results',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{#
    The manifest is emitted by data_quality__logical_test_results_part_N, which
    are implementation details rather than consumer contracts. Splitting it on
    whole flag models keeps each generated statement inside Athena's
    262,144-byte query-string limit; this relation stays the stable public
    interface and is a thin union over the parts.
#}

{% set part_models = [] %}
{% for chunk_index in range(the_tuva_project.dq_logical_chunk_count()) %}
    {% do part_models.append(ref('data_quality__logical_test_results_part_' ~ (chunk_index + 1))) %}
{% endfor %}

select
      cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(input_table_name as {{ dbt.type_string() }}) as input_table_name
    , cast(test_name as {{ dbt.type_string() }}) as test_name
    , cast(display_name as {{ dbt.type_string() }}) as display_name
    , cast(description as {{ dbt.type_string() }}) as description
    , cast(grain as {{ dbt.type_string() }}) as grain
    , cast(flag_table_name as {{ dbt.type_string() }}) as flag_table_name
    , cast(flag_column_name as {{ dbt.type_string() }}) as flag_column_name
    , cast(test_type as {{ dbt.type_string() }}) as test_type
    , cast(severity as {{ dbt.type_int() }}) as severity
    , cast(total_row_count as {{ dbt.type_bigint() }}) as total_row_count
    , cast(tested_count as {{ dbt.type_bigint() }}) as tested_count
    , cast(failed_count as {{ dbt.type_bigint() }}) as failed_count
    , cast(passed_count as {{ dbt.type_bigint() }}) as passed_count
    , cast(not_applicable_count as {{ dbt.type_bigint() }}) as not_applicable_count
from (
    {% for part_model in part_models %}
    select
          data_source
        , input_table_name
        , test_name
        , display_name
        , description
        , grain
        , flag_table_name
        , flag_column_name
        , test_type
        , severity
        , total_row_count
        , tested_count
        , failed_count
        , passed_count
        , not_applicable_count
    from {{ part_model }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
) as logical_test_results
