{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with member_months as (
    select
        year_month
      , count(1) as member_months
    from {{ ref('core__member_months') }}
    group by year_month
)

, service_categories as (
    select
        cast(c.year_month_int as {{ dbt.type_string() }}) as year_month
      , m.service_category_1
      , m.service_category_2
      , sum(m.paid_amount) as total_paid
    from {{ ref('core__medical_claim') }} as m
    left join {{ ref('reference_data__calendar') }} as c
      on m.claim_start_date = c.full_date
    group by
        cast(c.year_month_int as {{ dbt.type_string() }})
      , m.service_category_1
      , m.service_category_2
)

select
    scat.year_month
  , scat.service_category_1
  , scat.service_category_2
  , scat.total_paid
  , scat.total_paid / mm.member_months as pmpm
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from service_categories as scat
inner join member_months as mm
  on scat.year_month = mm.year_month
