{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

with mm_distinct as (
  select distinct person_id, data_source, payer, {{ quote_column('plan') }}
  from {{ ref('core__member_months') }}
)

select
  p.data_source,
  m.payer,
  m.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  coalesce(p.brand_vs_generic, '(unknown)') as brand_vs_generic,
  sum(coalesce(p.paid_amount,0)) as spend,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__pharmacy') }} p
left join mm_distinct m
  on p.person_id = m.person_id and p.data_source = m.data_source
group by p.data_source, m.payer, m.{{ quote_column('plan') }}, coalesce(p.brand_vs_generic, '(unknown)')

