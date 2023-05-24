{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select *
from {{ ref('acute_inpatient_institutional') }}

union

select *
from {{ ref('dialysis_institutional') }}

union

select *
from {{ ref('emergency_department_institutional') }}

union

select *
from {{ ref('home_health_institutional') }}

union

select *
from {{ ref('hospice_institutional') }}

union

select *
from {{ ref('lab_institutional') }}

union

select *
from {{ ref('outpatient_hospital_or_clinic_institutional') }}

union

select *
from {{ ref('outpatient_psychiatric_institutional') }}

union

select *
from {{ ref('skilled_nursing_institutional') }}

union

select *
from {{ ref('urgent_care_institutional') }}