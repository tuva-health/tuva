{{ config(severity='error', tags=['data_quality','exploratory_charts','coverage']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
),
expected(graph_name) as (
  select 'medical_paid_amount_vs_end_date_matrix' union all
  select 'medical_claim_count_vs_end_date_matrix' union all
  select 'medical_claim_paid_over_time_yearly'    union all
  select 'medical_claim_volume_over_time_yearly'  union all
  select 'pharmacy_paid_amount_vs_dispensing_date_matrix' union all
  select 'pharmacy_claim_count_vs_dispensing_date_matrix' union all
  select 'pharmacy_claim_paid_over_time_yearly'   union all
  select 'pharmacy_claim_volume_over_time_yearly' union all
  select 'medical_claims_with_eligibility'        union all
  select 'professional_claims_yearly_percentage'  union all
  select 'institutional_claims_yearly_percentage' union all
  select 'pharmacy_claims_yearly_percentage'
),
missing as (
  select e.graph_name
  from expected e
  left join (select distinct graph_name from final) f
    on e.graph_name = f.graph_name
  where f.graph_name is null
)
select * from missing