{{ config(
    enabled = var('claims_enabled', false) | as_bool,
    tags = ['claims_preprocessing', 'member_month']
) }}

with spine as (
  select
      year_month
    , first_day_of_month
    , last_day_of_month
  from {{ ref('member_month__month_spine') }}
)

, summary as (
  select
      count(*) as row_count
    , count(distinct year_month) as distinct_month_count
    , min(first_day_of_month) as minimum_month
    , max(first_day_of_month) as maximum_month
    , max(case when year_month = '200002' then last_day_of_month end) as leap_february_end
    , max(case when year_month = '210002' then last_day_of_month end) as non_leap_february_end
  from spine
)

, ordered as (
  select
      first_day_of_month
    , last_day_of_month
    , lag(first_day_of_month) over (order by first_day_of_month) as previous_month
  from spine
)

select 'invalid supported range or row count' as failure
from summary
where row_count <> 2412
   or distinct_month_count <> 2412
   or minimum_month <> cast('1900-01-01' as date)
   or maximum_month <> cast('2100-12-01' as date)
   or leap_february_end <> cast('2000-02-29' as date)
   or non_leap_february_end <> cast('2100-02-28' as date)

union all

select 'month spine contains a gap or incomplete month' as failure
from ordered
where (
       previous_month is not null
       and {{ dbt.datediff('previous_month', 'first_day_of_month', 'month') }} <> 1
      )
   or last_day_of_month <> cast(
        {{ dbt.dateadd(
            datepart='day',
            interval=-1,
            from_date_or_timestamp=dbt.dateadd(
                datepart='month',
                interval=1,
                from_date_or_timestamp='first_day_of_month'
            )
        ) }}
        as date
      )
