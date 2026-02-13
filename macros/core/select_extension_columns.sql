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
#}
    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}

    {%- set passthrough_config = var('passthrough', {}) -%}
    {%- set effective_prefix = prefix if prefix is not none else passthrough_config.get('prefix', 'x_') -%}
    {%- set effective_strip_prefix = strip_prefix if strip_prefix is not none else passthrough_config.get('strip', false) -%}

    {#- Get columns from the relation -#}
    {%- set source_columns = adapter.get_columns_in_relation(relation) -%}

    {%- if source_columns | length == 0 -%}
        {{ return('') }}
    {%- endif -%}

    {%- set alias_prefix = alias ~ '.' if alias else '' -%}
    {%- set extension_columns = [] -%}

    {#- Find extension columns -#}
    {%- for col in source_columns -%}
        {%- if col.name.lower().startswith(effective_prefix.lower()) -%}
            {%- set stripped_name = col.name[effective_prefix | length:] -%}
            {%- if effective_strip_prefix -%}
                {%- set col_expr = alias_prefix ~ col.name ~ ' as ' ~ stripped_name -%}
            {%- else -%}
                {%- set col_expr = alias_prefix ~ col.name -%}
            {%- endif -%}
            {%- do extension_columns.append(col_expr) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if extension_columns | length > 0 -%}
        {%- for col_expr in extension_columns %}
    , {{ col_expr }}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
