{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with pharmacy_claim as (
  select
      c.year_month_int
    , p.claim_id
    , max(case when p.paid_date is not null then 1 else 0 end) as non_null_paid_date
    , max(case when p.dispensing_date is not null then 1 else 0 end) as non_null_dispensing_date
  from {{ ref('pharmacy_claim') }} p
  left join {{ ref('reference_data__calendar') }} c
    on coalesce(p.paid_date, p.dispensing_date) = c.full_date
  group by
      p.claim_id
    , c.year_month_int
)

select
    year_month_int as year_month
  , sum(non_null_paid_date) as paid_date
  , sum(non_null_dispensing_date) as dispensing_date
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from pharmacy_claim
group by
    year_month_int
