{{ config(materialized='view', tags='medical economics') }}

with bookend_dates as (
select 
    patient_id
,   coverage_start_date
,   coverage_end_date
,   to_date(date_part(year,coverage_start_date) || '-' || date_part(month,coverage_start_date) || '-' || '01', 'YYYY-MM-DD' ) as min_date
,   case
        when coverage_end_date is null then to_date(date_part(year,getdate()) || '-' || date_part(month,getdate()) || '-' || '01', 'YYYY-MM-DD' )
        else to_date(date_part(year,coverage_end_date) || '-' || date_part(month,coverage_end_date) || '-' || '01', 'YYYY-MM-DD' ) 
    end max_date
from {{ ref('stg_coverage') }}
)

select distinct
    a.patient_id
,   b.member_date
,   b.member_month
,   b.member_year
,   row_number() over(partition by patient_id order by member_date) as sequence
from bookend_dates a
left join {{ ref('member_months_lookup') }} b
    on a.min_date <= b.member_date
    and a.max_date >= b.member_date
