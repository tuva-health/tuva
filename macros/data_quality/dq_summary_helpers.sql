{% macro dq_enabled_input_layer_model_names() %}
    {% set model_names = [] %}

    {% if var('clinical_enabled', false) | as_bool %}
        {% do model_names.extend([
            'input_layer__appointment',
            'input_layer__condition',
            'input_layer__encounter',
            'input_layer__immunization',
            'input_layer__lab_result',
            'input_layer__location',
            'input_layer__medication',
            'input_layer__observation',
            'input_layer__patient',
            'input_layer__practitioner',
            'input_layer__procedure'
        ]) %}
    {% endif %}

    {% if var('claims_enabled', false) | as_bool %}
        {% do model_names.extend([
            'input_layer__eligibility',
            'input_layer__medical_claim',
            'input_layer__pharmacy_claim'
        ]) %}
    {% endif %}

    {% if (var('provider_attribution_enabled', false) and var('claims_enabled', false)) | as_bool %}
        {% do model_names.append('input_layer__provider_attribution') %}
    {% endif %}

    {{ return(model_names) }}
{% endmacro %}

{% macro dq_expected_input_layer_models() %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {% set enabled_model_names = dq_enabled_input_layer_model_names() %}
    {% set models = [] %}
    {% for model_name in enabled_model_names %}
        {% set model_node = dq_find_model_node(model_name) %}
        {% if model_node is not none %}
            {% do models.append(model_node) %}
        {% endif %}
    {% endfor %}
    {{ return(models) }}
{% endmacro %}

{% macro dq_find_model_node(model_name) %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% for graph_node in graph['nodes'].values() %}
        {% if graph_node.resource_type == 'model'
              and graph_node.package_name == 'the_tuva_project'
              and graph_node.name == model_name %}
            {{ return(graph_node) }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}

{% macro dq_actual_relation(node) %}
    {% if node is none %}
        {{ return(none) }}
    {% endif %}

    {{ return(
        adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias
        )
    ) }}
{% endmacro %}

{% macro dq_actual_columns(relation) %}
    {% if relation is none %}
        {{ return([]) }}
    {% endif %}

    {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}

{% macro dq_has_column(columns, column_name) %}
    {% set requested_name = column_name | lower %}
    {% for column in columns %}
        {% if column.name | lower == requested_name %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}
    {{ return(false) }}
{% endmacro %}

{% macro dq_expected_columns(node) %}
    {% set expected_columns = [] %}

    {% for column in node.columns.values() %}
        {% set meta = column.config.meta if column.config is not none and column.config.meta is not none else {} %}
        {% do expected_columns.append(
            {
                'name': column.name | lower,
                'data_type': meta.get('data_type'),
                'is_primary_key': meta.get('is_primary_key', false),
                'column_order': loop.index
            }
        ) %}
    {% endfor %}

    {{ return(expected_columns) }}
{% endmacro %}

{% macro dq_type_families_match_sql(expected_type_family_expression, actual_type_family_expression) %}
    (
        {{ expected_type_family_expression }} = {{ actual_type_family_expression }}
        or (
            {{ expected_type_family_expression }} = 'boolean'
            and {{ actual_type_family_expression }} = 'integer'
        )
        or (
            {{ expected_type_family_expression }} = 'numeric'
            and {{ actual_type_family_expression }} = 'integer'
        )
    )
{% endmacro %}

{% macro dq_expected_pk_columns(node) %}
    {% set pk_columns = [] %}

    {% for column in dq_expected_columns(node) %}
        {% if column['is_primary_key'] %}
            {% do pk_columns.append(column['name']) %}
        {% endif %}
    {% endfor %}

    {{ return(pk_columns) }}
{% endmacro %}

{% macro dq_source_key_sentinel() %}
    {{ return('__dq_null__') }}
{% endmacro %}

{% macro dq_empty_row_sql() %}
    select 1 as _dq_empty_row
{% endmacro %}

{% macro dq_empty_result_guard_sql() %}
    from (
        {{ dq_empty_row_sql() }}
    ) as dq_empty_row
    where 1 = 0
{% endmacro %}

{% macro dq_source_row_count_sql(relation) %}
    {% set actual_columns = dq_actual_columns(relation) %}

    select
          {% if dq_has_column(actual_columns, 'data_source') %}
          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
          {% else %}
          '{{ dq_source_key_sentinel() }}'
          {% endif %} as data_source_key
        , cast(count(*) as {{ dbt.type_numeric() }}) as row_count
    from {{ relation }}
    {% if dq_has_column(actual_columns, 'data_source') %}
    group by coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
    {% endif %}
{% endmacro %}

{% macro dq_source_dimension_sql(relation) %}
    {% set actual_columns = dq_actual_columns(relation) %}

    {% if dq_has_column(actual_columns, 'data_source') %}
        select distinct
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , cast(data_source as {{ dbt.type_string() }}) as data_source
        from {{ relation }}

        union all

        select
              '{{ dq_source_key_sentinel() }}' as data_source_key
            , cast(null as {{ dbt.type_string() }}) as data_source
        from (
            {{ dq_empty_row_sql() }}
        ) as dq_empty_source
        where not exists (
            select 1
            from {{ relation }}
        )
    {% else %}
        select
              '{{ dq_source_key_sentinel() }}' as data_source_key
            , cast(null as {{ dbt.type_string() }}) as data_source
    {% endif %}
{% endmacro %}

{% macro dq_missing_source_dimension_sql() %}
    select
          '{{ dq_source_key_sentinel() }}' as data_source_key
        , cast(null as {{ dbt.type_string() }}) as data_source
{% endmacro %}

{% macro dq_source_key_expression_sql(relation, relation_alias='source_rows') %}
    {% set actual_columns = dq_actual_columns(relation) %}

    {% if dq_has_column(actual_columns, 'data_source') %}
        {{ return("coalesce(cast(" ~ relation_alias ~ ".data_source as " ~ dbt.type_string() ~ "), '" ~ dq_source_key_sentinel() ~ "')") }}
    {% else %}
        {{ return("'" ~ dq_source_key_sentinel() ~ "'") }}
    {% endif %}
{% endmacro %}

{% macro dq_base_type_family(type_string) %}
    {% if type_string is none %}
        {{ return('unknown') }}
    {% endif %}

    {% set normalized = type_string | lower | trim %}
    {% set compact = normalized | replace(' ', '') %}
    {% set base = normalized.split('(')[0] | trim %}

    {% if base in ['varchar', 'nvarchar', 'string', 'text', 'char', 'character', 'character varying'] %}
        {{ return('string') }}
    {% elif base in ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'int64'] %}
        {{ return('integer') }}
    {% elif base in ['boolean', 'bool', 'bit'] %}
        {{ return('boolean') }}
    {% elif base == 'date' %}
        {{ return('date') }}
    {% elif 'timestamp' in base or base in ['datetime', 'datetime2', 'smalldatetime'] %}
        {{ return('timestamp') }}
    {% elif base in ['number', 'numeric', 'decimal', 'float', 'float4', 'float8', 'double', 'double precision', 'real', 'float64', 'bignumeric'] %}
        {% if '(' in compact and ')' in compact and ',' in compact %}
            {% set scale = compact.split('(', 1)[1].split(')', 1)[0].split(',')[-1] | int %}
            {% if scale == 0 %}
                {{ return('integer') }}
            {% endif %}
        {% endif %}
        {{ return('numeric') }}
    {% else %}
        {{ return(base) }}
    {% endif %}
{% endmacro %}

{% macro dq_type_family(type_string) %}
    {{ return(adapter.dispatch('dq_type_family', 'the_tuva_project')(type_string)) }}
{% endmacro %}

{% macro default__dq_type_family(type_string) %}
    {{ return(dq_base_type_family(type_string)) }}
{% endmacro %}

{% macro bigquery__dq_type_family(type_string) %}
    {% if type_string is none %}
        {{ return('unknown') }}
    {% endif %}

    {% set normalized = type_string | lower | trim %}
    {% if normalized == 'bool' %}
        {{ return('boolean') }}
    {% elif normalized == 'bytes' %}
        {{ return('string') }}
    {% else %}
        {{ return(dq_base_type_family(normalized)) }}
    {% endif %}
{% endmacro %}

{% macro fabric__dq_type_family(type_string) %}
    {% if type_string is none %}
        {{ return('unknown') }}
    {% endif %}

    {% set normalized = type_string | lower | trim %}
    {% if normalized in ['nvarchar', 'varchar', 'char', 'nchar'] %}
        {{ return('string') }}
    {% else %}
        {{ return(dq_base_type_family(normalized)) }}
    {% endif %}
{% endmacro %}
