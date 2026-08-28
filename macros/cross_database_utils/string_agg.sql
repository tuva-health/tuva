{#

    Concatenates a string expression across a group.

    A thin wrapper over `dbt.listagg`, which already carries the per-adapter
    spellings. The arguments are passed through unchanged, so `delimiter` is
    a quoted SQL literal ("','") and `order_by` is a full clause
    ("order by ordinal"), exactly as `dbt.listagg` expects them.

#}

{% macro string_agg(expression, delimiter, order_by=none) %}
  {{ return(adapter.dispatch('string_agg', 'the_tuva_project')(expression, delimiter, order_by)) }}
{% endmacro %}

{% macro default__string_agg(expression, delimiter, order_by=none) %}
  {{ dbt.listagg(measure=expression, delimiter_text=delimiter, order_by_clause=order_by) }}
{% endmacro %}
