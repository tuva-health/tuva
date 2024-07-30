{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combine_header_models as 
(
select
  claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__acute_inpatient_institutional_other') }}

union all

select
  claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__acute_inpatient_institutional_maternity') }}

union all

select
  claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__acute_inpatient_institutional_med_surg') }}

union all

select
  claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__acute_inpatient_institutional_substance_use') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__ambulatory_surgery_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__dialysis_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__emergency_department_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__home_health_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_hospice_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__lab_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__observation_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_hospital_or_clinic_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_pharmacy_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_physical_therapy_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_psychiatric_institutional') }}



union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_skilled_nursing_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__urgent_care_institutional') }}

union all

select   claim_id
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_psychiatric_institutional') }}


)



select
  h.claim_id
, h.service_category_2
, h.service_category_3
, source_model_name
from combine_header_models h

