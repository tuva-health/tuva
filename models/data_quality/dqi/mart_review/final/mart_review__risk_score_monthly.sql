{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool,
    materialized = 'table'
) }}

-- Monthly average payment risk score by data_source/payer/plan using collection_end_date as the month anchor
with monthly as (
  select
    s.person_id,
    s.payment_year,
    s.collection_end_date,
    s.payment_risk_score,
    s.payment_risk_score_weighted_by_months,
    s.member_months
  from {{ ref('cms_hcc__patient_risk_scores_monthly') }} s
)
, mm as (
  select person_id, data_source, payer, {{ quote_column('plan') }} as {{ quote_column('plan') }}, year_month
  from {{ ref('core__member_months') }}
)
, joined as (
  select
    mm.data_source,
    mm.payer,
    mm.{{ quote_column('plan') }} as {{ quote_column('plan') }},
    cast(mm.year_month as {{ dbt.type_string() }}) as year_month,
    monthly.payment_risk_score_weighted_by_months as w_score,
    monthly.member_months as months
  from monthly
  join mm
    on monthly.person_id = mm.person_id
   and cast(
         {{ concat_custom([
              date_part('year', 'monthly.collection_end_date'),
              "case when " ~ date_part('month', 'monthly.collection_end_date') ~ " < 10 then '0' else '' end",
              date_part('month', 'monthly.collection_end_date')
         ]) }} as {{ dbt.type_string() }}
       ) = cast(mm.year_month as {{ dbt.type_string() }})
)
select
  data_source,
  payer,
  {{ quote_column('plan') }},
  year_month,
  case when sum(months) = 0 then null else sum(w_score) / sum(months) end as avg_risk_score,
  '{{ var('tuva_last_run') }}' as tuva_last_run
from joined
group by data_source, payer, {{ quote_column('plan') }}, year_month

