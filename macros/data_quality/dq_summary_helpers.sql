{% macro dq_enabled_input_layer_model_domains() %}
    {% set domains = [] %}

    {% if var('clinical_enabled', false) | as_bool %}
        {% do domains.append({
            'name': 'clinical',
            'model_names': [
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
            ]
        }) %}
    {% endif %}

    {% if var('claims_enabled', false) | as_bool %}
        {% set claims_model_names = [
            'input_layer__eligibility',
            'input_layer__medical_claim',
            'input_layer__pharmacy_claim'
        ] %}

        {% if var('provider_attribution_enabled', false) | as_bool %}
            {% do claims_model_names.append('input_layer__provider_attribution') %}
        {% endif %}

        {% do domains.append({
            'name': 'claims',
            'model_names': claims_model_names
        }) %}
    {% endif %}

    {{ return(domains) }}
{% endmacro %}

{% macro dq_enabled_input_layer_model_names() %}
    {% set model_names = [] %}

    {% for domain in dq_enabled_input_layer_model_domains() %}
        {% do model_names.extend(domain['model_names']) %}
    {% endfor %}

    {{ return(model_names) }}
{% endmacro %}

{% macro dq_enabled_input_layer_model_names_for_domain(domain_name) %}
    {% for domain in dq_enabled_input_layer_model_domains() %}
        {% if domain['name'] == domain_name %}
            {{ return(domain['model_names']) }}
        {% endif %}
    {% endfor %}

    {{ return([]) }}
{% endmacro %}

{% macro dq_input_layer_domain_name(model_name) %}
    {% for domain in dq_enabled_input_layer_model_domains() %}
        {% if model_name in domain['model_names'] %}
            {{ return(domain['name']) }}
        {% endif %}
    {% endfor %}

    {{ exceptions.raise_compiler_error(
        "Structural Data Quality could not determine the Input Layer domain for enabled Wrapper '"
        ~ model_name
        ~ "'. Add the Wrapper to dq_enabled_input_layer_model_domains()."
    ) }}
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

{% macro dq_required_actual_relation(node) %}
    {% set relation = dq_actual_relation(node) %}

    {% if relation is none %}
        {{ exceptions.raise_compiler_error(
            "Structural Data Quality could not locate the Warehouse Table or View for Input Layer Wrapper '"
            ~ node.name
            ~ "'. Build the enabled Input Layer Models and Input Layer Wrappers before running Structural Data Quality."
        ) }}
    {% endif %}

    {{ return(relation) }}
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

{% macro dq_actual_column(columns, column_name, relation_name='Warehouse Table or View') %}
    {% set requested_name = column_name | lower %}
    {% set matches = [] %}

    {% for column in columns %}
        {% if column.name | lower == requested_name %}
            {% do matches.append(column) %}
        {% endif %}
    {% endfor %}

    {% if matches | length > 1 %}
        {{ exceptions.raise_compiler_error(
            "Structural Data Quality found more than one physical column in "
            ~ relation_name
            ~ " that normalizes to '"
            ~ requested_name
            ~ "'. Rename the ambiguous columns before running Structural Data Quality."
        ) }}
    {% endif %}

    {{ return(matches[0] if matches | length == 1 else none) }}
{% endmacro %}

{% macro dq_supported_type_families() %}
    {{ return(['string', 'integer', 'numeric', 'boolean', 'date', 'timestamp']) }}
{% endmacro %}

{% macro dq_expected_columns(node) %}
    {% set expected_columns = [] %}
    {% set normalized_names = [] %}
    {% set duplicate_names = [] %}
    {% set missing_data_types = [] %}
    {% set unsupported_data_types = [] %}
    {% set primary_key_columns = [] %}
    {% set data_source_count = namespace(value=0) %}

    {% for column in node.columns.values() %}
        {% set meta = column.config.meta if column.config is not none and column.config.meta is not none else {} %}
        {% set normalized_name = column.name | lower %}
        {% set expected_type = meta.get('data_type') %}
        {% set is_primary_key = meta.get('is_primary_key', false) | as_bool %}

        {% if normalized_name in normalized_names %}
            {% do duplicate_names.append(normalized_name) %}
        {% else %}
            {% do normalized_names.append(normalized_name) %}
        {% endif %}

        {% if expected_type is none or expected_type | trim == '' %}
            {% do missing_data_types.append(normalized_name) %}
        {% elif dq_type_family(expected_type) not in dq_supported_type_families() %}
            {% do unsupported_data_types.append(normalized_name ~ '=' ~ expected_type) %}
        {% endif %}

        {% if is_primary_key %}
            {% do primary_key_columns.append(normalized_name) %}
        {% endif %}

        {% if normalized_name == 'data_source' %}
            {% set data_source_count.value = data_source_count.value + 1 %}
        {% endif %}

        {% do expected_columns.append(
            {
                'name': normalized_name,
                'data_type': expected_type,
                'is_primary_key': is_primary_key,
                'column_order': loop.index
            }
        ) %}
    {% endfor %}

    {% set contract_errors = [] %}
    {% if duplicate_names | length > 0 %}
        {% do contract_errors.append('duplicate case-normalized columns: ' ~ (duplicate_names | unique | list | join(', '))) %}
    {% endif %}
    {% if missing_data_types | length > 0 %}
        {% do contract_errors.append('columns missing meta.data_type: ' ~ (missing_data_types | join(', '))) %}
    {% endif %}
    {% if unsupported_data_types | length > 0 %}
        {% do contract_errors.append('unsupported portable data types: ' ~ (unsupported_data_types | join(', '))) %}
    {% endif %}
    {% if primary_key_columns | length == 0 %}
        {% do contract_errors.append('no columns are marked meta.is_primary_key') %}
    {% endif %}
    {% if data_source_count.value != 1 %}
        {% do contract_errors.append('expected exactly one data_source column but found ' ~ data_source_count.value) %}
    {% elif 'data_source' not in primary_key_columns %}
        {% do contract_errors.append('data_source is not marked meta.is_primary_key') %}
    {% endif %}

    {% if contract_errors | length > 0 %}
        {{ exceptions.raise_compiler_error(
            "Invalid Structural Data Quality contract for Input Layer Wrapper '"
            ~ node.name
            ~ "': "
            ~ (contract_errors | join('; '))
            ~ ". Correct Tuva Core's Input Layer YAML metadata."
        ) }}
    {% endif %}

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

{% macro dq_structural_null_source_key() %}
    {{ return('__dq_structural_null__') }}
{% endmacro %}

{% macro dq_structural_source_value_prefix() %}
    {{ return('__dq_structural_value__:') }}
{% endmacro %}

{% macro dq_structural_source_key_sql(data_source_expression) %}
    {% set non_null_key = dbt.concat([
        "'" ~ dq_structural_source_value_prefix() ~ "'",
        "cast(" ~ data_source_expression ~ " as " ~ dbt.type_string() ~ ")"
    ]) %}

    {{ return(
        "case when "
        ~ data_source_expression
        ~ " is null then '"
        ~ dq_structural_null_source_key()
        ~ "' else "
        ~ non_null_key
        ~ " end"
    ) }}
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
        {{ return('binary') }}
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
