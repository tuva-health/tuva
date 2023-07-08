{{ config(
     enabled = var('member_months_enabled',var('tuva_marts_enabled',True))
   )
}}

with month_start_and_end_dates as (
select 
  concat(cast(year as {{ dbt.type_string() }} )
    ,lpad(cast(month as {{ dbt.type_string() }}),2,'0')) 
    as year_month
, min(full_date) as month_start_date
, max(full_date) as month_end_date
from {{ ref('terminology__calendar')}}
group by 1
)

select distinct
  a.patient_id
, year_month
, a.payer
, '{{ var('last_update')}}' as last_update
from {{ ref('member_months__stg_eligibility') }} a
inner join month_start_and_end_dates b
  on a.enrollment_start_date <= b.month_end_date
  and a.enrollment_end_date >= b.month_start_date

