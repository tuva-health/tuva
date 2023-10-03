{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with duplicate_bill_types as (
select distinct
  claim_id
, 'Other' as service_category_2
from {{ ref('service_category__duplicate_bill_types') }}
)

, combine as (
select *
from {{ ref('service_category__acute_inpatient_institutional') }}

union all

select *
from {{ ref('service_category__dialysis_institutional') }}

union all

select *
from {{ ref('service_category__emergency_department_institutional') }}

union all

select *
from {{ ref('service_category__home_health_institutional') }}

union all

select *
from {{ ref('service_category__hospice_institutional') }}

union all

select *
from {{ ref('service_category__lab_institutional') }}

union all

select *
from {{ ref('service_category__outpatient_hospital_or_clinic_institutional') }}

union all

select *
from {{ ref('service_category__outpatient_psychiatric_institutional') }}

union all

select *
from {{ ref('service_category__skilled_nursing_institutional') }}

union all

select *
from {{ ref('service_category__urgent_care_institutional') }}
)

select
  claim_id
, service_category_2
from duplicate_bill_types

union all

select
  a.claim_id
, a.service_category_2
from combine a
left join duplicate_bill_types b
  on a.claim_id = b.claim_id
where b.claim_id is null

