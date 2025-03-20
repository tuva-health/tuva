{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with range_cte as (
  select
      min(claim_start_date) as first_date
    {% if target.type == 'fabric' %}
      , getdate() as last_date
    {% else %}
      , current_date as last_date
    {% endif %}
  from {{ ref('input_layer__medical_claim') }} p
)

, date_cte as (
  select distinct
      year_month_int
  from {{ ref('reference_data__calendar') }} c
  inner join range_cte r
    on c.full_date between r.first_date and r.last_date
)

, medical_claim as (
  select
      c.year_month_int
    , count(distinct p.claim_id) as claim_volume
    , sum(p.paid_amount) as paid_amount
  from {{ ref('input_layer__medical_claim') }} p
  left join {{ ref('reference_data__calendar') }} c
    on p.claim_start_date = c.full_date
  group by
      c.year_month_int
)

select
    d.year_month_int as year_month
  , coalesce(claim_volume, 0) as claim_volume
  , coalesce(paid_amount, 0) as paid_amount
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from date_cte d
left join medical_claim m
  on d.year_month_int = m.year_month_int
