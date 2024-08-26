{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__acute_inpatient_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__ambulatory_surgery_professional') }}


union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__dialysis_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__emergency_department_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__home_health_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_hospice_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_psychiatric_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_rehab_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__lab_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__office_based_other_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__office_based_pharmacy_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__office_based_physical_therapy_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__office_based_radiology') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__office_based_surgery_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__office_based_visit_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_hospital_or_clinic_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_psychiatric_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_rehab_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__inpatient_skilled_nursing_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__urgent_care_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_hospice_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_pharmacy_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_substance_use_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_physical_therapy_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_radiology_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__observation_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__dme_professional') }}

union all

select     
      claim_id
    , claim_line_number
    , claim_line_id
    , service_category_2
    , service_category_3
    , tuva_last_run
, source_model_name
from {{ ref('service_category__ambulance_professional') }}
