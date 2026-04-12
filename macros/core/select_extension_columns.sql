{% macro select_extension_columns(relation, alias=none, prefix=none, strip_prefix=none) %}
{#
    Selects extension columns from a relation.

    Extension columns are identified by a prefix (default: 'x_').
    This macro reads column metadata directly from the provided relation
    and outputs the extension columns, optionally stripping the prefix.

    Arguments:
        relation: The relation to read extension columns from (typically input_layer__*)
        alias: Optional table alias for column references
        prefix: Column prefix to identify extensions (default: passthrough.prefix var)
        strip_prefix: Whether to strip prefix (default: passthrough.strip var)

    Configuration in dbt_project.yml:
        vars:
          passthrough:
            prefix: 'x_'    # Prefix to identify passthrough columns
            strip: false    # Whether to strip prefix in final output

    Unit-test override:
        In dbt unit tests the macro uses adapter.get_columns_in_relation() at
        compile time. Because dbt resolves ref() to an ephemeral CTE during unit
        test compilation, the introspection call returns nothing. Set the project
        var _extension_columns_override to a list of column names to bypass DB
        introspection entirely and make tests self-contained:

            overrides:
              vars:
                _extension_columns_override: ['x_temp_person_id', 'x_temp_first_name']
#}
    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}

    {%- set passthrough_config = var('passthrough', {}) -%}
    {%- set effective_prefix = prefix if prefix is not none else passthrough_config.get('prefix', 'x_') -%}
    {%- set effective_strip_prefix = strip_prefix if strip_prefix is not none else passthrough_config.get('strip', false) -%}
    {%- set alias_prefix = alias ~ '.' if alias else '' -%}
    {%- set extension_columns = [] -%}

    {#- Allow an explicit column-name list to be supplied via project var.
        Intended for dbt unit tests: set _extension_columns_override in the
        test's `overrides.vars` block so the test does not rely on DB
        introspection and is fully self-contained. -#}
    {%- set col_override = var('_extension_columns_override', none) -%}

    {%- if col_override is not none -%}

        {%- for col_name in col_override -%}
            {%- if col_name.lower().startswith(effective_prefix.lower()) -%}
                {%- if effective_strip_prefix -%}
                    {%- set stripped = col_name[effective_prefix | length:] -%}
                    {%- do extension_columns.append(alias_prefix ~ col_name ~ ' as ' ~ stripped) -%}
                {%- else -%}
                    {%- do extension_columns.append(alias_prefix ~ col_name) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}

    {%- else -%}

        {#- Normal path: introspect the relation at compile time. -#}
        {%- set source_columns = adapter.get_columns_in_relation(relation) -%}

        {%- if source_columns | length == 0 -%}
            {{ return('') }}
        {%- endif -%}

        {%- for col in source_columns -%}
            {%- if col.name.lower().startswith(effective_prefix.lower()) -%}
                {%- set stripped_name = col.name[effective_prefix | length:] -%}
                {%- if effective_strip_prefix -%}
                    {%- do extension_columns.append(alias_prefix ~ col.name ~ ' as ' ~ stripped_name) -%}
                {%- else -%}
                    {%- do extension_columns.append(alias_prefix ~ col.name) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}

    {%- endif -%}

    {%- for col_expr in extension_columns %}
    , {{ col_expr }}
    {%- endfor -%}
{% endmacro %}
