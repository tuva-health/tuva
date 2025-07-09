with enrollment__stg_eligibility as (
    select *
    from {{ ref('the_tuva_project', 'enrollment__stg_eligibility') }}
),
calendar as (
    select *
    from {{ ref('the_tuva_project', 'enrollment__stg_calendar') }}
)
select distinct
    {{ dbt_utils.generate_surrogate_key(['data_source', 'member_id', 'payer', 'plan', 'year_month']) }} as member_month_sk
    , a.data_source
    , a.member_id
    , a.payer
    , a.{{ quote_column('plan') }}
    , b.year_month
    , b.first_day_of_month as month_start_date
    , b.last_day_of_month as month_end_date
from enrollment__stg_eligibility as a
inner join calendar as b
  on a.enrollment_start_date <= b.last_day_of_month
  and a.enrollment_end_date >= b.first_day_of_month
