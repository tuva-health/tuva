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
  coalesce(cast(p.sex as {{ dbt.type_string() }}), '(unknown)') as gender,
  count(distinct p.person_id) as count_members,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__patient') }} p
left join mm_distinct m
  on p.person_id = m.person_id and p.data_source = m.data_source
where p.sex is not null
group by p.data_source, m.payer, m.{{ quote_column('plan') }}, gender

