{{ config(
    enabled = var('claims_enabled', var('clinical_enabled', var('tuva_marts_enabled', False))) | as_bool,
    materialized = 'table'
) }}

with mm_distinct as (
  select distinct person_id, data_source, payer, {{ quote_column('plan') }}
  from {{ ref('core__member_months') }}
)

select
  t.data_source,
  m.payer,
  m.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  t.condition,
  count(distinct t.person_id) as count_members,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__tuva_chronic_conditions') }} t
left join mm_distinct m
  on t.person_id = m.person_id and t.data_source = m.data_source
group by t.data_source, m.payer, m.{{ quote_column('plan') }}, t.condition

