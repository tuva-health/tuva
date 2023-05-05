{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with valid_eligibility_rows as (
select
  patient_id,
  enrollment_start_date,
  enrollment_end_date,

  cast({{ dbt.date_trunc("month", "enrollment_start_date") }}
    as date) as floor_enrollment_start_date,

  cast({{ dbt.last_day("enrollment_end_date", "month") }}
    as date
  ) as ceil_enrollment_end_date

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
),


medical_eligibility_flags as (
select
  claim_id,
  claim_line_number,
  patient_id,
  max(had_eligibility_flag) as had_eligibility_flag
from (
    select
      aa.claim_id as claim_id,
      aa.claim_line_number as claim_line_number,
      aa.patient_id as patient_id,
      case
        when bb.patient_id is not null then 1
        else 0
      end as had_eligibility_flag
    from {{ ref('core__medical_claim') }} aa
         left join valid_eligibility_rows bb
         on aa.patient_id = bb.patient_id
         and aa.claim_start_date
             between bb.floor_enrollment_start_date and bb.ceil_enrollment_end_date
)
group by claim_id, claim_line_number, patient_id
),


pharmacy_eligibility_flags as (
select
  claim_id,
  claim_line_number,
  patient_id,
  max(had_eligibility_flag) as had_eligibility_flag
from (
    select
      aa.claim_id as claim_id,
      aa.claim_line_number as claim_line_number,
      aa.patient_id as patient_id,
      case
        when bb.patient_id is not null then 1
        else 0
      end as had_eligibility_flag
    from {{ ref('core__pharmacy_claim') }} aa
         left join valid_eligibility_rows bb
         on aa.patient_id = bb.patient_id
         and aa.dispensing_date
             between bb.floor_enrollment_start_date and bb.ceil_enrollment_end_date
)
group by claim_id, claim_line_number, patient_id
),


medical_claims as (
select
  cast(aa.claim_id as {{ dbt.type_string() }} ) as claid_id,
  cast(aa.claim_line_number as integer) as claim_line_number,
  cast(aa.claim_type as {{ dbt.type_string() }} ) as claim_type,
  cast(aa.patient_id as {{ dbt.type_string() }} ) as patient_id,
  'medical' as medical_or_pharmacy,
  cast(aa.service_category_1 as {{ dbt.type_string() }} ) as service_category_1,
  cast(aa.service_category_2 as {{ dbt.type_string() }} ) as service_category_2,
  cast(bb.year_month as {{ dbt.type_string() }} ) as year_month,
  cast(bb.member_month_count as integer) as member_month_count,
  cast(aa.paid_amount as numeric) as paid_amount,
  cast(cc.had_eligibility_flag as integer) as had_eligibility_flag
  
from {{ ref('core__medical_claim') }} aa

     left join member_months bb
     on substring(cast(aa.claim_start_date as {{ dbt.type_string() }}), 1, 7) = bb.year_month

     left join medical_eligibility_flags cc
     on aa.claim_id = cc.claim_id
     and aa.claim_line_number = cc.claim_line_number
     and aa.patient_id = cc.patient_id
),


pharmacy_claims as (
select
  cast(aa.claim_id  as {{ dbt.type_string() }} )as claid_id,
  cast(aa.claim_line_number as integer ) as claim_line_number,
  'pharmacy' as claim_type,
  cast(aa.patient_id as {{ dbt.type_string() }} ) as patient_id,
  'pharmacy' as medical_or_pharmacy,
  cast(null   as {{ dbt.type_string() }} ) as service_category_1,
  cast(null  as {{ dbt.type_string() }} ) as service_category_2,
  cast(bb.year_month  as {{ dbt.type_string() }} ) as year_month,
  cast(bb.member_month_count as integer) as member_month_count,
  cast(aa.paid_amount as numeric) as paid_amount,
  cast(cc.had_eligibility_flag as integer) as had_eligibility_flag
  
from {{ ref('core__pharmacy_claim') }} aa

     left join member_months bb
     on substring(cast(aa.dispensing_date as {{ dbt.type_string() }}), 1, 7) = bb.year_month

     left join pharmacy_eligibility_flags cc
     on aa.claim_id = cc.claim_id
     and aa.claim_line_number = cc.claim_line_number
     and aa.patient_id = cc.patient_id
),


pmpm_output as (
select * from medical_claims
union all
select * from pharmacy_claims
)


select *
from pmpm_output
