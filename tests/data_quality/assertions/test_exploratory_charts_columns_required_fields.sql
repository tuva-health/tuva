-- framework: dbt data tests (SQL). Return rows that FAIL the test.
{{ config(
    severity='error',
    tags=['data_quality','exploratory_charts','columns']
) }}

with final as (
  {% if target.type == 'fabric' %}
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
  {% else %}
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
  {% endif %}
),
expected as (
  select
    cast(null as {{ dbt.type_string() }}) as data_quality_category,
    cast(null as {{ dbt.type_string() }}) as graph_name,
    cast(null as {{ dbt.type_string() }}) as level_of_detail,
    cast(null as {{ dbt.type_string() }}) as y_axis_description,
    cast(null as {{ dbt.type_string() }}) as x_axis_description,
    cast(null as {{ dbt.type_string() }}) as filter_description,
    cast(null as {{ dbt.type_string() }}) as sum_description,
    cast(null as {{ dbt.type_string() }}) as y_axis,
    cast(null as {{ dbt.type_string() }}) as x_axis,
    cast(null as {{ dbt.type_string() }}) as chart_filter,
    cast(0 as {{ dbt.type_numeric() }}) as value
),
checks as (
  select
    case when data_quality_category is null then 'data_quality_category' end as missing_col,
    *
  from final
  where data_quality_category is null
  union all
  select case when graph_name is null then 'graph_name' end, *
  from final
  where graph_name is null
  union all
  select case when level_of_detail is null then 'level_of_detail' end, *
  from final
  where level_of_detail is null
  union all
  select case when x_axis is null then 'x_axis' end, *
  from final
  where x_axis is null
)
select * from checks