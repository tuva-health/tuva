{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with eligibility_spans as(
    select distinct
        {{ dbt.concat([
            "member_id",
            "'-'",
            "enrollment_start_date",
            "'-'",
            "enrollment_end_date",
            "'-'",
            "payer",
            "'-'",
            quote_column('plan'),
        ]) }} as eligibility_span_id
        , enrollment_start_date
        , enrollment_end_date
    from {{ ref('eligibility') }}
)
, month_start_and_end_dates as (
  select
    {{ dbt.concat(["year",
                  dbt.right(dbt.concat(["'0'", "month"]), 2)]) }} as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar')}}
  group by year, month, year_month
)
, member_months as (
    select distinct
        eligibility_span_id
        , year_month
    from eligibility_spans es
    inner join month_start_and_end_dates d
        on es.enrollment_start_date <= d.month_end_date
        and es.enrollment_end_date >= d.month_start_date
)


select
    year_month
    , count(distinct eligibility_span_id) as member_months
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from member_months
group by
    year_month
