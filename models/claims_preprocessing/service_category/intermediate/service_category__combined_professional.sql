{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with combined as (
select *
from {{ ref('service_category__acute_inpatient_professional') }}

union all

select *
from {{ ref('service_category__ambulatory_surgery_professional') }}

union all

select *
from {{ ref('service_category__dialysis_professional') }}

union all

select *
from {{ ref('service_category__emergency_department_professional') }}

union all

select *
from {{ ref('service_category__home_health_professional') }}

union all

select *
from {{ ref('service_category__hospice_professional') }}

union all

select *
from {{ ref('service_category__inpatient_psychiatric_professional') }}

union all

select *
from {{ ref('service_category__inpatient_rehab_professional') }}

union all

select *
from {{ ref('service_category__lab_professional') }}

union all

select *
from {{ ref('service_category__office_visit_professional') }}

union all

select *
from {{ ref('service_category__outpatient_hospital_or_clinic_professional') }}

union all

select *
from {{ ref('service_category__outpatient_psychiatric_professional') }}

union all

select *
from {{ ref('service_category__outpatient_rehab_professional') }}

union all

select *
from {{ ref('service_category__skilled_nursing_professional') }}

union all

select *
from {{ ref('service_category__urgent_care_professional') }}
)

select 
  claim_id
, claim_line_number
, service_category_2
, tuva_last_run
from {{ ref('service_category__dme_professional') }}

union all

select 
  a.claim_id
, a.claim_line_number
, a.service_category_2
, a.tuva_last_run
from {{ ref('service_category__ambulance_professional') }} a
left join {{ ref('service_category__dme_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where (b.claim_id is null and b.claim_line_number is null)

union all

select 
  a.claim_id
, a.claim_line_number
, a.service_category_2
, a.tuva_last_run
from combined a
left join {{ ref('service_category__dme_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
left join {{ ref('service_category__ambulance_professional') }} c
  on a.claim_id = c.claim_id
  and a.claim_line_number = c.claim_line_number
where (b.claim_id is null and b.claim_line_number is null)
  and (c.claim_id is null and c.claim_line_number is null)