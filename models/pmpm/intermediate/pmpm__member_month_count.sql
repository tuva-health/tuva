{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with valid_eligibility_rows as (
select
  patient_id,
  enrollment_start_date,
  enrollment_end_date,


  cast( {{ dbt.date_trunc( "month", "enrollment_start_date") }} as date)   as floor_enrollment_start_date,
  cast( {{ dbt.last_day("enrollment_end_date", "month") }} as date)    as ceil_enrollment_end_date

from {{ ref('core__eligibility') }}
where patient_id is not null
and enrollment_start_date is not null
and enrollment_end_date is not null
and enrollment_start_date <= enrollment_end_date
),


all_claim_dates as (
select
  claim_start_date as claim_date,
  patient_id as patient_id
from {{ ref('core__medical_claim') }}

union all

select
  dispensing_date as claim_date,
  patient_id as patient_id
from {{ ref('core__pharmacy_claim') }}
),


member_months as (
select
  year_month,
  count(distinct patient_id) as member_month_count
from (
    select
      aa.claim_date as claim_date,
      aa.patient_id as patient_id,
      substring(cast(aa.claim_date as {{ dbt.type_string() }}), 1, 7) as year_month
    from all_claim_dates aa
         inner join valid_eligibility_rows bb
         on aa.patient_id = bb.patient_id
         and aa.claim_date
             between bb.floor_enrollment_start_date and bb.ceil_enrollment_end_date
)
group by year_month
)


select *
from member_months
