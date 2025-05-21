{#
    This macro loops through a list of columns to dynamically build a JSON object
    for the adapters the Tuva package supports.

    The following parameters are required:
     - group_by_col: column to group rows (e.g. 'claim_id'), this can be a list
     - order_by_col: column to sort rows within the JSON object
       (e.g. 'eobItemSequence'), this can be a list, if "order_by_col=none"
       then JSON object not ordered
     - object_col_list: list of columns to build the JSON object
       (e.g. ['eobItemSequence','eobItemRevenueCode'])
     - object_col_name: column name for the nbew JSON object column
       (e.g. 'eob_item_sequence')
     - table_ref: model reference or CTE name
#}


{#
    This macro changes columns from snake case to camel case,
    required for nested json objects.
#}
{% macro snake_to_camel(s) %}
    {%- set parts = s.split('_') -%}
    {%- set output = [] -%}
    {%- for i in range(0, parts | length) -%}
        {%- if i == 0 -%}
            {%- do output.append(parts[i]) -%}
        {%- else -%}
            {%- do output.append(parts[i].capitalize()) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(output | join('')) }}
{% endmacro %}

{% macro create_json_object(
        table_ref,
        group_by_col,
        object_col_name,
        object_col_list
        ) %}
  {{ return(adapter.dispatch('create_json_object')(table_ref, group_by_col, object_col_name, object_col_list)) }}
{% endmacro %}

/* default */
{% macro default__create_json_object(table_ref, group_by_col, object_col_name, object_col_list) %}
  {{ exceptions.raise_compiler_error("The macro create_json_object is not implemented for this adapter.") }}
{% endmacro %}

/* snowflake */
{% macro snowflake__create_json_object(table_ref, group_by_col, object_col_name, object_col_list) %}
select
    {{ group_by_col }}
    , to_json(
        array_agg(
            object_construct(
                {%- for col in object_col_list %}
                {% if not loop.first %}, {% endif -%}
                '{{ snake_to_camel(col) }}',
                {%- if 'list' in col | lower -%}
                parse_json( {{ col }} ) /* Parse JSON lists to prevent escaping */
                {%- else -%}
                {{ col }}
                {%- endif -%}
                {%- endfor %}
            )
        )
    ) as {{ object_col_name }}
from {{ table_ref }}
group by {{ group_by_col }}
{% endmacro %}

/* bigquery */
{% macro bigquery__create_json_object(table_ref, group_by_col, object_col_name, object_col_list) %}
select
    {{ group_by_col }}
    , to_json_string(
        array_agg(
            struct(
                {%- for col in object_col_list %}
                {% if not loop.first %}, {% endif -%}
                {%- if 'list' in col | lower -%}
                parse_json( {{ col }} ) as {{ snake_to_camel(col) }} /* Parse JSON lists to prevent escaping */
                {%- else -%}
                {{ col }} as {{ snake_to_camel(col) }}
                {%- endif %}
                {%- endfor %}
            )
        )
    ) as {{ object_col_name }}
from {{ table_ref }}
group by {{ group_by_col }}
{% endmacro %}

/* redshift */
{% macro redshift__create_json_object(table_ref, group_by_col, object_col_name, object_col_list) %}
select
    {{ group_by_col }},
    '[' || listagg(json_serialize(object_json), ',') || ']' as {{ object_col_name }}
from (
    select
        {{ group_by_col }},
        object(
            {%- for col in object_col_list %}
                {%- if not loop.first %}, {% endif -%}
                '{{ snake_to_camel(col) }}',
                {%- if 'list' in col | lower -%}
                json_parse( {{ col }} ) /* Parse JSON lists to prevent escaping */
                {%- else -%}
                {{ col }}
                {%- endif %}
            {% endfor %}
        ) as object_json
    from {{ table_ref }}
) sub
group by {{ group_by_col }}
{% endmacro %}


/* fabric */
