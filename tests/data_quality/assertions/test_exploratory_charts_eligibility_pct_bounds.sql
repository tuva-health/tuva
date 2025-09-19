{{ config(severity='error', tags=['data_quality','exploratory_charts','bounds']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
)
select *
from final
where graph_name = 'medical_claims_with_eligibility'
  and (value < 0 or value > 100)