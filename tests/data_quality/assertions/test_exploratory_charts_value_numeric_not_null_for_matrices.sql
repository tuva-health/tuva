{{ config(severity='error', tags=['data_quality','exploratory_charts','types']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
),
targets as (
  select *
  from final
  where graph_name in (
    'medical_paid_amount_vs_end_date_matrix',
    'medical_claim_count_vs_end_date_matrix',
    'pharmacy_paid_amount_vs_dispensing_date_matrix',
    'pharmacy_claim_count_vs_dispensing_date_matrix',
    'medical_claim_paid_over_time_yearly',
    'medical_claim_volume_over_time_yearly',
    'pharmacy_claim_paid_over_time_yearly',
    'pharmacy_claim_volume_over_time_yearly',
    'professional_claims_yearly_percentage',
    'institutional_claims_yearly_percentage',
    'pharmacy_claims_yearly_percentage'
  )
)
select *
from targets
where value is null