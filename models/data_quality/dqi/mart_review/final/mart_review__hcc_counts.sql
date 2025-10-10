{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

with mm_distinct as (
  select distinct person_id, data_source, payer, {{ quote_column('plan') }}
  from {{ ref('core__member_months') }}
)

select
  x.data_source,
  m.payer,
  m.{{ quote_column('plan') }} as {{ quote_column('plan') }},
  f.hcc_code,
  count(distinct f.person_id) as count_members,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__patient_risk_factors') }} f
left join {{ ref('mart_review__patient') }} x
  on f.person_id = x.person_id
left join mm_distinct m
  on f.person_id = m.person_id and x.data_source = m.data_source
where f.hcc_code is not null
group by x.data_source, m.payer, m.{{ quote_column('plan') }}, f.hcc_code

