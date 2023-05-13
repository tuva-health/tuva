{% macro count_nulls(column_name) %}
  sum(case when {{ column_name }} is null then 1 else 0 end) as null_values
{% endmacro %}

{% macro pct_nulls(column_name) %}
  avg(case when {{ column_name }} is null then 1 else 0 end) as null_pct
{% endmacro %}

{% macro descriptive_stats(column_name) %}
  , count(*) as row_count
  , sum(case when {{ column_name }} is null then 1 else 0 end) as null_values
  , avg(case when {{ column_name }} is null then 1 else 0 end) as null_pct
  , avg({{ column_name }}) as mean
  , min({{ column_name }}) as min
  , percentile_disc(0.01) within group (order by {{ column_name }}) as pct_01
  , percentile_disc(0.05) within group (order by {{ column_name }}) as pct_05
  , percentile_disc(0.10) within group (order by {{ column_name }}) as pct_10
  , percentile_disc(0.25) within group (order by {{ column_name }}) as pct_25
  , percentile_disc(0.50) within group (order by {{ column_name }}) as pct_50
  , percentile_disc(0.75) within group (order by {{ column_name }}) as pct_75
  , percentile_disc(0.90) within group (order by {{ column_name }}) as pct_90
  , percentile_disc(0.95) within group (order by {{ column_name }}) as pct_95
  , percentile_disc(0.99) within group (order by {{ column_name }}) as pct_99
  , max({{ column_name }}) as max
{% endmacro %}
