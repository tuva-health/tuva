{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select *
from {{ ref('acute_inpatient_professional') }}

union

select *
from {{ ref('ambulance_professional') }}

union

select *
from {{ ref('ambulatory_surgery_professional') }}

union

select *
from {{ ref('dialysis_professional') }}

union

select *
from {{ ref('dme_professional') }}

union

select *
from {{ ref('emergency_department_professional') }}

union

select *
from {{ ref('home_health_professional') }}

union

select *
from {{ ref('hospice_professional') }}

union

select *
from {{ ref('inpatient_psychiatric_professional') }}

union

select *
from {{ ref('inpatient_rehab_professional') }}

union

select *
from {{ ref('lab_professional') }}

union

select *
from {{ ref('office_visit_professional') }}

union

select *
from {{ ref('outpatient_hospital_or_clinic_professional') }}

union

select *
from {{ ref('outpatient_psychiatric_professional') }}

union

select *
from {{ ref('outpatient_rehab_professional') }}

union

select *
from {{ ref('skilled_nursing_professional') }}

union

select *
from {{ ref('urgent_care_professional') }}