{% macro smart_union(relations, source_index='_source') %}
{#
    Unions relations with automatic alignment of columns.
    Missing columns are filled with NULL. Columns matching the configured extension prefix are sorted last.

    smart_union vs dbt_utils.union_relations:
    ┌─────────────────────┬─────────────────────────────────────┬─────────────────────────┐
    │ Feature             │ dbt_utils.union_relations           │ smart_union             │
    ├─────────────────────┼─────────────────────────────────────┼─────────────────────────┤
    │ Source tracking     │ Full path string                    │ Numeric index (1, 2...) │
    │ Filter syntax       │ WHERE _source LIKE '%table_name%'   │ WHERE _source = 1       │
    │ Column ordering     │ Arbitrary                           │ Core first, prefix last │
    └─────────────────────┴─────────────────────────────────────┴─────────────────────────┘

    Arguments:
        relations: List of relations to union
        source_index: Source tracking column name (default: '_source'). Set to none to disable.

    Unit-test override:
        dbt unit-test refs compile to ephemeral CTEs that cannot be introspected
        reliably. `_smart_union_columns_override` supplies the complete shared
        column list for tests whose mocked relations have the same columns.

    Usage:
        {{ smart_union([ref('stg_claims'), ref('stg_clinical')]) }}
        -- Adds _source column: 1 = first relation, 2 = second, etc.
        -- Filter with: WHERE _source = 1

        -- Without source tracking:
        {{ smart_union([ref('stg_claims'), ref('stg_clinical')], source_index=none) }}
#}

{%- set passthrough_config = get_extension_passthrough_config() -%}
{%- set passthrough_prefix = passthrough_config['prefix'] | lower -%}

{%- if not execute -%}
    {{ return('') }}
{%- endif -%}

{%- set columns_override = var('_smart_union_columns_override', none) -%}
{%- if columns_override is not none -%}
    {%- if columns_override is string
        or columns_override is mapping
        or columns_override is not sequence -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid `_smart_union_columns_override`: expected a list of column-name strings."
        ) -%}
    {%- endif -%}
    {%- if columns_override | length == 0 -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid `_smart_union_columns_override`: expected at least one column name."
        ) -%}
    {%- endif -%}
    {%- set override_names_by_lower = {} -%}
    {%- for column_name in columns_override -%}
        {%- if column_name is not string or column_name | length == 0 -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid `_smart_union_columns_override`: every item must be a "
                ~ "non-empty column-name string."
            ) -%}
        {%- endif -%}
        {%- set column_name_lower = column_name | lower -%}
        {%- if column_name_lower in override_names_by_lower -%}
            {%- do exceptions.raise_compiler_error(
                "Invalid `_smart_union_columns_override`: column names `"
                ~ override_names_by_lower[column_name_lower] ~ "` and `" ~ column_name
                ~ "` are equal case-insensitively."
            ) -%}
        {%- endif -%}
        {%- do override_names_by_lower.update({column_name_lower: column_name}) -%}
    {%- endfor -%}
{%- endif -%}

{%- set all_columns = {} -%}
{%- if columns_override is not none -%}
    {%- for column_name in columns_override -%}
        {%- do all_columns.update({
            column_name | lower: {
                'name': column_name,
                'data_type': dbt.type_string()
            }
        }) -%}
    {%- endfor -%}
{%- else -%}
    {%- for relation in relations -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- for col in cols -%}
            {%- if col.name.lower() not in all_columns -%}
                {%- do all_columns.update({col.name.lower(): col}) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
{%- endif -%}

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
    {%- if columns_override is not none -%}
        {%- set relation_cols = columns_override | map('lower') | list -%}
    {%- else -%}
        {%- set relation_cols = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- endif -%}

    select
    {%- if source_index %}
        {{ loop.index }} as {{ source_index }},
    {%- endif %}
    {%- for col in sorted_columns %}
        {%- if col.name.lower() in relation_cols %}
        {#- Overrides are an internal unit-test hook and do not carry the
            adapter-normalized identifier casing returned by introspection. -#}
        {{ quote_column(col.name) if columns_override is not none else adapter.quote(col.name) }}
        {%- else %}
        cast(null as {{ col.data_type }}) as {{ adapter.quote(col.name) }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
    from {{ relation }}

    {%- if not loop.last %}
    union all
    {% endif -%}
{%- endfor -%}

{% endmacro %}
