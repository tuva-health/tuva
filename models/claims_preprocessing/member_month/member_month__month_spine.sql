{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

-- Complete package-owned month spine for Tuva's supported date range.
-- 1900-01 through 2100-12 is 2,412 consecutive months. The inline digit
-- relations avoid recursive and nested CTEs so the model remains portable.
{% set digit_rows %}
  select 0 as digit
  union all select 1
  union all select 2
  union all select 3
  union all select 4
  union all select 5
  union all select 6
  union all select 7
  union all select 8
  union all select 9
{% endset %}

select
    {{ yyyymm('month_starts.first_day_of_month') }} as year_month
  , cast({{ yyyymm('month_starts.first_day_of_month') }} as {{ dbt.type_int() }}) as year_month_int
  , cast({{ date_part('year', 'month_starts.first_day_of_month') }} as {{ dbt.type_int() }}) as year
  , cast({{ date_part('month', 'month_starts.first_day_of_month') }} as {{ dbt.type_int() }}) as month
  , month_starts.first_day_of_month
  , cast(
      {{ dbt.dateadd(
          datepart='day',
          interval=-1,
          from_date_or_timestamp=dbt.dateadd(
              datepart='month',
              interval=1,
              from_date_or_timestamp='month_starts.first_day_of_month'
          )
      ) }}
      as date
    ) as last_day_of_month
from (
  select
    cast(
      {{ dbt.dateadd(
          datepart='month',
          interval='generated_months.month_offset',
          from_date_or_timestamp="cast('1900-01-01' as date)"
      ) }}
      as date
    ) as first_day_of_month
  from (
    select cast(
        ones.digit
        + (tens.digit * 10)
        + (hundreds.digit * 100)
        + (thousands.digit * 1000)
      as {{ dbt.type_int() }}) as month_offset
    from ({{ digit_rows }}) as ones
    cross join ({{ digit_rows }}) as tens
    cross join ({{ digit_rows }}) as hundreds
    cross join ({{ digit_rows }}) as thousands
  ) as generated_months
  where generated_months.month_offset < 2412
) as month_starts
