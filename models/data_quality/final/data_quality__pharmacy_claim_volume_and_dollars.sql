{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

select
    c.year_month_int as year_month
  , count(distinct p.claim_id) as claim_volume
  , sum(p.paid_amount) as paid_amount
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('input_layer__pharmacy_claim') }} p
left join {{ ref('reference_data__calendar') }} c
  on coalesce(p.paid_date, p.dispensing_date) = c.full_date
group by
    c.year_month_int
