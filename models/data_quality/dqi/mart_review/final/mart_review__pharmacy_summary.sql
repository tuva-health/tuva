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
  coalesce(p.atc_3_name, p.atc_2_name) as theraclass,
  p.ndc_code,
  coalesce(p.ndc_description, p.rxnorm_description) as ndc_description,
  sum(coalesce(p.paid_amount,0)) as spend,
  avg(nullif(p.days_supply,0)) as avg_days_supply,
  avg(nullif(p.quantity,0)) as avg_quantity,
  avg(nullif(p.refills,0)) as avg_refills,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('mart_review__pharmacy') }} p
left join mm_distinct m
  on p.person_id = m.person_id and p.data_source = m.data_source
group by
  p.data_source,
  m.payer,
  m.{{ quote_column('plan') }},
  coalesce(p.atc_3_name, p.atc_2_name),
  p.ndc_code,
  coalesce(p.ndc_description, p.rxnorm_description)
