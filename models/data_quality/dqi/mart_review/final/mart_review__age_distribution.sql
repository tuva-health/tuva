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
  case
    when p.age < 5 then '00-04'
    when p.age < 18 then '05-17'
    when p.age < 30 then '18-29'
    when p.age < 40 then '30-39'
    when p.age < 50 then '40-49'
    when p.age < 60 then '50-59'
    when p.age < 70 then '60-69'
    when p.age < 80 then '70-79'
    else '80+'
  end as age_bucket,
  count(distinct p.person_id) as count_members,
  avg(p.age) as avg_age,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__patient') }} p
left join mm_distinct m
  on p.person_id = m.person_id and p.data_source = m.data_source
group by p.data_source, m.payer, m.{{ quote_column('plan') }}, age_bucket

