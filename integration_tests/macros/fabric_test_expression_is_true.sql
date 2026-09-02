{#
  dbt-fabric provides this adapter override in the dbt package, but the
  integration project's intentionally narrow dbt_utils dispatch order does
  not search that package. Keep the override local and Fabric-specific so the
  generated test CTE has a named column without changing other adapters.
#}
{% macro fabric__test_expression_is_true(model, expression, column_name=none, condition='1=1') %}

{% set column_list = '*' if should_store_failures() else '1 as col' %}

select
    {{ column_list }}
from {{ model }}
{% if condition and condition is not none %}
where {{ condition }}
and (
{%- else %}
where (
{%- endif %}
{% if column_name is none %}
    not({{ expression }})
{%- else %}
    not({{ column_name }} {{ expression }})
{%- endif %}
)

{% endmacro %}


{#
  SQL Server is T-SQL and needs the same named-column CTE, but dbt-sqlserver
  registers no adapter-type parent, so fabric__ is not reached by dispatch.
#}
{% macro sqlserver__test_expression_is_true(model, expression, column_name=none, condition='1=1') %}
    {{ return(fabric__test_expression_is_true(model, expression, column_name, condition)) }}
{% endmacro %}
