{% macro get_extension_passthrough_config(prefix=none, strip_prefix=none) %}
{#
    Returns the validated extension-column configuration.

    `passthrough` must be a mapping, `prefix` must be a non-empty string, and
    `strip` must be a real boolean. Macro arguments override the corresponding
    configured value, but do not make an otherwise invalid project
    configuration valid.
#}
    {%- set passthrough_config = var('passthrough', {}) -%}

    {%- if passthrough_config is not mapping -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid `passthrough` configuration: expected a mapping with optional "
            ~ "`prefix` and `strip` keys."
        ) -%}
    {%- endif -%}

    {%- set configured_prefix = passthrough_config.get('prefix', 'x_') -%}
    {%- if configured_prefix is not string or configured_prefix | trim | length == 0 -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid `passthrough.prefix`: expected a non-empty string."
        ) -%}
    {%- endif -%}

    {%- set configured_strip = passthrough_config.get('strip', false) -%}
    {%- if configured_strip is not boolean -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid `passthrough.strip`: expected a YAML boolean (`true` or `false`), "
            ~ "not a string or another truthy value."
        ) -%}
    {%- endif -%}

    {%- if prefix is not none -%}
        {%- if prefix is not string or prefix | trim | length == 0 -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid `prefix` argument to `select_extension_columns`: expected a "
                ~ "non-empty string."
            ) -%}
        {%- endif -%}
        {%- set effective_prefix = prefix -%}
    {%- else -%}
        {%- set effective_prefix = configured_prefix -%}
    {%- endif -%}

    {%- if strip_prefix is not none -%}
        {%- if strip_prefix is not boolean -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid `strip_prefix` argument to `select_extension_columns`: "
                ~ "expected a boolean (`true` or `false`)."
            ) -%}
        {%- endif -%}
        {%- set effective_strip_prefix = strip_prefix -%}
    {%- else -%}
        {%- set effective_strip_prefix = configured_strip -%}
    {%- endif -%}

    {{ return({
        'prefix': effective_prefix,
        'strip': effective_strip_prefix
    }) }}
{% endmacro %}


{% macro _extension_declared_fixed_columns(relation) %}
{#
    Builds a case-insensitive map of fixed columns declared for the current
    model and, when resolvable, for the relation being inspected. Core model
    declarations protect computed output names that do not exist in the source
    relation; Input Layer declarations catch overly broad prefixes before they
    can duplicate fixed columns in the Normalized Layer.
#}
    {%- set fixed_columns = {} -%}

    {%- if model is defined and model is mapping -%}
        {%- set current_columns = model.get('columns', {}) -%}
        {%- if current_columns is mapping -%}
            {%- for column_name in current_columns.keys() -%}
                {%- do fixed_columns.update({
                    column_name | lower: {
                        'name': column_name,
                        'location': "model `" ~ model.get('name', 'unknown') ~ "`"
                    }
                }) -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}

    {%- if relation is not none
        and relation.identifier is defined
        and graph is defined
        and graph is mapping
        and graph.get('nodes', {}) is mapping -%}
        {%- set relation_identifier = relation.identifier | lower -%}
        {%- set relation_database = relation.database | lower if relation.database is not none else none -%}
        {%- set relation_schema = relation.schema | lower if relation.schema is not none else none -%}
        {%- for node in graph.get('nodes', {}).values() -%}
            {%- if node is mapping
                and node.get('resource_type') == 'model'
                and node.get('alias', node.get('name', '')) | lower == relation_identifier
                and (node.get('database') | lower if node.get('database') is not none else none) == relation_database
                and (node.get('schema') | lower if node.get('schema') is not none else none) == relation_schema -%}
                {%- set relation_columns = node.get('columns', {}) -%}
                {%- if relation_columns is mapping -%}
                    {%- for column_name in relation_columns.keys() -%}
                        {%- if column_name | lower not in fixed_columns -%}
                            {%- do fixed_columns.update({
                                column_name | lower: {
                                    'name': column_name,
                                    'location': "referenced model `" ~ node.get('name', 'unknown') ~ "`"
                                }
                            }) -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {{ return(fixed_columns) }}
{% endmacro %}


{% macro select_extension_columns(relation, alias=none, prefix=none, strip_prefix=none) %}
{#
    Selects extension columns from a relation.

    Extension columns are identified case-insensitively by a prefix (default:
    `x_`). The macro reads column metadata from the relation and emits the
    matching columns. The prefix can optionally be stripped in the output.

    Arguments:
        relation: The relation to inspect.
        alias: Optional SQL table alias used to qualify column references.
        prefix: Optional explicit prefix; defaults to `passthrough.prefix`.
        strip_prefix: Optional boolean; defaults to `passthrough.strip`.

    Configuration in dbt_project.yml:
        vars:
          passthrough:
            prefix: 'x_'
            strip: false

    Unit-test override:
        dbt unit-test refs compile to ephemeral CTEs that cannot be introspected
        reliably. `_extension_columns_override` supplies the source column names
        for those tests. Include fixed columns too when testing collision errors.
#}
    {%- set passthrough = get_extension_passthrough_config(prefix, strip_prefix) -%}
    {%- set effective_prefix = passthrough['prefix'] -%}
    {%- set effective_strip_prefix = passthrough['strip'] -%}

    {%- if alias is not none
        and (alias is not string or alias | trim | length == 0) -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid `alias` argument to `select_extension_columns`: expected a "
            ~ "non-empty string or `none`."
        ) -%}
    {%- endif -%}

    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}

    {%- set alias_prefix = alias ~ '.' if alias else '' -%}
    {%- set prefix_lower = effective_prefix | lower -%}
    {%- set extension_columns = [] -%}
    {%- set source_column_names = [] -%}
    {%- set using_override = false -%}

    {%- set declared_fixed_columns = _extension_declared_fixed_columns(relation) -%}

    {#- A prefix must never classify a declared fixed model column as an extension. -#}
    {%- for fixed_name_lower, fixed_column in declared_fixed_columns.items() -%}
        {%- if fixed_name_lower.startswith(prefix_lower) -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid `passthrough.prefix` `" ~ effective_prefix ~ "`: it classifies "
                ~ "fixed column `" ~ fixed_column['name'] ~ "` in "
                ~ fixed_column['location'] ~ " as an extension column. Choose a more "
                ~ "specific prefix."
            ) -%}
        {%- endif -%}
    {%- endfor -%}

    {#- Unit tests can bypass relation introspection with an explicit list. -#}
    {%- set col_override = var('_extension_columns_override', none) -%}
    {%- if col_override is not none -%}
        {%- if col_override is string
            or col_override is mapping
            or col_override is not sequence -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid `_extension_columns_override`: expected a list of column-name strings."
            ) -%}
        {%- endif -%}

        {%- set using_override = true -%}
        {%- for column_name in col_override -%}
            {%- if column_name is not string or column_name | length == 0 -%}
                {%- do exceptions.raise_compiler_error(
                    "Invalid `_extension_columns_override`: every item must be a "
                    ~ "non-empty column-name string."
                ) -%}
            {%- endif -%}
            {%- do source_column_names.append(column_name) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for column in adapter.get_columns_in_relation(relation) -%}
            {%- do source_column_names.append(column.name) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if source_column_names | length == 0 -%}
        {{ return('') }}
    {%- endif -%}

    {%- set source_names_by_lower = {} -%}
    {%- for column_name in source_column_names -%}
        {%- set column_name_lower = column_name | lower -%}
        {%- if column_name_lower.startswith(prefix_lower) -%}
            {%- if column_name_lower in source_names_by_lower -%}
                {%- do exceptions.raise_compiler_error(
                    "Extension-column discovery for relation `" ~ relation ~ "` found "
                    ~ "multiple extension columns that are equal case-insensitively: `"
                    ~ source_names_by_lower[column_name_lower] ~ "` and `" ~ column_name
                    ~ "`. Extension column names must be unique case-insensitively."
                ) -%}
            {%- endif -%}
            {%- do source_names_by_lower.update({column_name_lower: column_name}) -%}
        {%- endif -%}
    {%- endfor -%}

    {#- Only columns declared by the current output model reserve final names.
        A non-extension column on the inspected source relation may be omitted
        from the current model, so it must not block a valid stripped alias. -#}
    {%- set fixed_output_names = {} -%}
    {%- if model is defined and model is mapping -%}
        {%- set current_columns = model.get('columns', {}) -%}
        {%- if current_columns is mapping -%}
            {%- for column_name in current_columns.keys() -%}
                {%- do fixed_output_names.update({
                    column_name | lower: {
                        'name': column_name,
                        'location': "model `" ~ model.get('name', 'unknown') ~ "`"
                    }
                }) -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}

    {%- set extension_output_names = {} -%}
    {%- for column_name in source_column_names -%}
        {%- set column_name_lower = column_name | lower -%}
        {%- if column_name_lower.startswith(prefix_lower) -%}
            {%- if effective_strip_prefix -%}
                {%- set output_name = column_name[effective_prefix | length:] -%}
            {%- else -%}
                {%- set output_name = column_name -%}
            {%- endif -%}

            {%- if output_name | length == 0 -%}
                {%- do exceptions.raise_compiler_error(
                    "Extension column `" ~ column_name ~ "` on relation `" ~ relation
                    ~ "` becomes an empty output name when prefix `" ~ effective_prefix
                    ~ "` is stripped. Rename the extension column or disable stripping."
                ) -%}
            {%- endif -%}

            {%- set output_name_lower = output_name | lower -%}
            {%- if output_name_lower in fixed_output_names -%}
                {%- set collision = fixed_output_names[output_name_lower] -%}
                {%- do exceptions.raise_compiler_error(
                    "Extension column `" ~ column_name ~ "` on relation `" ~ relation
                    ~ "` resolves to output column `" ~ output_name ~ "`, which "
                    ~ "collides case-insensitively with fixed column `"
                    ~ collision['name'] ~ "` in " ~ collision['location'] ~ ". Rename "
                    ~ "the extension column, choose another prefix, or disable stripping."
                ) -%}
            {%- endif -%}

            {%- if output_name_lower in extension_output_names -%}
                {%- do exceptions.raise_compiler_error(
                    "Extension columns `" ~ extension_output_names[output_name_lower]
                    ~ "` and `" ~ column_name ~ "` on relation `" ~ relation
                    ~ "` both resolve case-insensitively to output column `" ~ output_name
                    ~ "`. Extension output names must be unique case-insensitively."
                ) -%}
            {%- endif -%}
            {%- do extension_output_names.update({output_name_lower: column_name}) -%}

            {#- Overrides are an internal unit-test hook and do not carry the
                adapter-normalized identifier casing returned by introspection. -#}
            {%- set source_identifier = column_name if using_override else adapter.quote(column_name) -%}
            {%- set source_expression = alias_prefix ~ source_identifier -%}
            {%- if effective_strip_prefix -%}
                {%- do extension_columns.append(
                    source_expression ~ ' as ' ~ adapter.quote(output_name)
                ) -%}
            {%- else -%}
                {%- do extension_columns.append(source_expression) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for column_expression in extension_columns %}
    , {{ column_expression }}
    {%- endfor -%}
{% endmacro %}
