{{ config(severity='error', tags=['data_quality','exploratory_charts','consistency']) }}

with final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
),
bad as (
  select *
  from final
  where
    (graph_name like '%count%' and sum_description not in ('unique_number_of_claims','count_distinct_claim_id'))
    or
    (graph_name like '%paid%' and sum_description not in ('total_paid_amount','paid_amount','total_paid'))
)
select * from bad