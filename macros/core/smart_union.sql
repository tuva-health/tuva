{% macro smart_union(relations, source_index='_source') %}
{#
    Unions relations with automatic alignment of columns.
    Missing columns are filled with NULL. Extension columns (prefixed) are sorted last.

    smart_union vs dbt_utils.union_relations:
    ┌─────────────────────┬─────────────────────────────────────┬─────────────────────────┐
    │ Feature             │ dbt_utils.union_relations           │ smart_union             │
    ├─────────────────────┼─────────────────────────────────────┼─────────────────────────┤
    │ Source tracking     │ Full path string                    │ Numeric index (1, 2...) │
    │ Filter syntax       │ WHERE _source LIKE '%table_name%'   │ WHERE _source = 1       │
    │ Column ordering     │ Arbitrary                           │ Core first, ext_ last   │
    └─────────────────────┴─────────────────────────────────────┴─────────────────────────┘

    Arguments:
        relations: List of relations to union
        source_index: Source tracking column name (default: '_source'). Set to none to disable.

    Usage:
        {{ smart_union([ref('stg_claims'), ref('stg_clinical')]) }}
        -- Adds _source column: 1 = first relation, 2 = second, etc.
        -- Filter with: WHERE _source = 1

        -- Without source tracking:
        {{ smart_union([ref('stg_claims'), ref('stg_clinical')], source_index=none) }}
#}

{%- if not execute -%}
    {{ return('') }}
{%- endif -%}

{%- set all_columns = {} -%}
{%- for relation in relations -%}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- for col in cols -%}
        {%- if col.name.lower() not in all_columns -%}
            {%- do all_columns.update({col.name.lower(): col}) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{#- Get passthrough prefix for column detection -#}
{%- set passthrough_config = var('passthrough', {}) -%}
{%- set passthrough_prefix = passthrough_config.get('prefix', 'x_').lower() -%}

{%- set core_cols = [] -%}
{%- set ext_cols = [] -%}
{%- for col_name, col in all_columns.items() -%}
    {#- Check if column has the configured passthrough prefix -#}
    {%- if col_name.startswith(passthrough_prefix) -%}
        {%- do ext_cols.append(col) -%}
    {%- else -%}
        {%- do core_cols.append(col) -%}
    {%- endif -%}
{%- endfor -%}
{%- set sorted_columns = core_cols + ext_cols -%}

{%- for relation in relations -%}
    {%- set relation_cols = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}

    select
    {%- if source_index %}
        {{ loop.index }} as {{ source_index }},
    {%- endif %}
    {%- for col in sorted_columns %}
        {%- if col.name.lower() in relation_cols %}
        {{ col.name }}
        {%- else %}
        cast(null as {{ col.data_type }}) as {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
    from {{ relation }}

    {%- if not loop.last %}
    union all
    {% endif -%}
{%- endfor -%}

{% endmacro %}
