{{ config(severity='error', tags=['data_quality','exploratory_charts','accepted_values']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
)
select *
from final
where level_of_detail not in ('month','year')