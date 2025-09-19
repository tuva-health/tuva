{{ config(severity='warn', tags=['data_quality','exploratory_charts','smoke']) }}

with src_counts as (
  select
    (select count(*) from {{ ref('input_layer__medical_claim') }}) as mc,
    (select count(*) from {{ ref('input_layer__pharmacy_claim') }}) as pc
),
final as (
  {{ test_utils.evaluate('tests/data_quality/test_exploratory_charts_columns.sql') }}
)
select 'no_rows' as reason
from final, src_counts
group by reason
having count(*) = 0 and (max(mc) > 0 or max(pc) > 0)