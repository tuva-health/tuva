{{ config(severity='error', tags=['data_quality','exploratory_charts','axis']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
)
select *
from final
where graph_name in (
  'medical_paid_amount_vs_end_date_matrix',
  'medical_claim_count_vs_end_date_matrix',
  'pharmacy_paid_amount_vs_dispensing_date_matrix',
  'pharmacy_claim_count_vs_dispensing_date_matrix'
)
and (y_axis_description is null or x_axis_description is null)