{{ config(severity='error', tags=['data_quality','exploratory_charts','uniqueness']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
),
dupes as (
  select
    graph_name, x_axis, y_axis, chart_filter,
    count(*) as n
  from final
  group by 1,2,3,4
  having count(*) > 1
)
select * from dupes