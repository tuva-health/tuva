{% macro select_null_extension_columns(
    relation,
    prefix=none,
    type_source_alias=none,
    _extension_source=none
) %}
{#
    Emits typed null expressions for every extension column on a relation.

    This is used when a derived source participates in a union with a direct
    source that owns extension columns. The derived rows keep the same schema
    without receiving values that cannot be traced to that source.
#}
    {%- set passthrough = get_extension_passthrough_config(prefix, false) -%}
    {%- set effective_prefix = passthrough['prefix'] -%}

    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}

    {%- set extension_source = _extension_source
        if _extension_source is not none
        else _get_extension_source_columns(relation, effective_prefix) -%}

    {%- for column in extension_source['columns'] -%}
        {%- if column['data_type'] is none -%}
            {%- if type_source_alias is not string or type_source_alias | trim | length == 0 -%}
                {%- do exceptions.raise_compiler_error(
                    "`select_null_extension_columns` requires `type_source_alias` "
                    ~ "when `_extension_columns_override` supplies extension columns."
                ) -%}
            {%- endif -%}
    , (
        select extension_type_source.{{ column['name'] }}
        from {{ type_source_alias }} as extension_type_source
        where 1 = 0
      ) as {{ column['name'] }}
        {%- else -%}
    , cast(null as {{ column['data_type'] }}) as {{ adapter.quote(column['name']) }}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}
