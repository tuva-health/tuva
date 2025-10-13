{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

with mm_distinct as (
  select distinct person_id, data_source, payer, {{ quote_column('plan') }}
  from {{ ref('core__member_months') }}
)
, base_claims as (
  select
    p.data_source,
    m.payer,
    m.{{ quote_column('plan') }} as {{ quote_column('plan') }},
    coalesce(p.atc_3_name, p.atc_2_name) as theraclass,
    p.ndc_code,
    coalesce(p.ndc_description, p.rxnorm_description) as ndc_description,
    p.paid_amount,
    p.days_supply,
    p.quantity,
    p.refills
  from {{ ref('mart_review__pharmacy') }} p
  left join mm_distinct m
    on p.person_id = m.person_id and p.data_source = m.data_source
)
, ndc_spend as (
  select data_source, payer, {{ quote_column('plan') }}, ndc_code, sum(coalesce(paid_amount,0)) as spend
  from base_claims
  group by data_source, payer, {{ quote_column('plan') }}, ndc_code
)
, ndc_rank as (
  select *, row_number() over (partition by data_source, payer, {{ quote_column('plan') }} order by spend desc) as rn
  from ndc_spend
)
, claims_with_rank as (
  select bc.*, nr.rn
  from base_claims bc
  join ndc_rank nr
    on bc.data_source = nr.data_source
   and bc.payer = nr.payer
   and bc.{{ quote_column('plan') }} = nr.{{ quote_column('plan') }}
   and bc.ndc_code = nr.ndc_code
)
, top_rows as (
  select
    data_source,
    payer,
    {{ quote_column('plan') }} as {{ quote_column('plan') }},
    theraclass,
    ndc_code,
    ndc_description,
    sum(coalesce(paid_amount,0)) as spend,
    avg(nullif(days_supply,0)) as avg_days_supply,
    avg(nullif(quantity,0)) as avg_quantity,
    avg(nullif(refills,0)) as avg_refills
  from claims_with_rank
  where rn <= 10
  group by data_source, payer, {{ quote_column('plan') }}, theraclass, ndc_code, ndc_description
)
, other_rows as (
  select
    data_source,
    payer,
    {{ quote_column('plan') }} as {{ quote_column('plan') }},
    cast('(various)' as {{ dbt.type_string() }}) as theraclass,
    cast('OTHER_NDC_OUTSIDE_PREVIEW' as {{ dbt.type_string() }}) as ndc_code,
    cast('Other NDCs outside preview' as {{ dbt.type_string() }}) as ndc_description,
    sum(coalesce(paid_amount,0)) as spend,
    avg(nullif(days_supply,0)) as avg_days_supply,
    avg(nullif(quantity,0)) as avg_quantity,
    avg(nullif(refills,0)) as avg_refills
  from claims_with_rank
  where rn > 10
  group by data_source, payer, {{ quote_column('plan') }}
)
select *, '{{ var('tuva_last_run') }}' as tuva_last_run
from (
  select * from top_rows
  union all
  select * from other_rows
) x

