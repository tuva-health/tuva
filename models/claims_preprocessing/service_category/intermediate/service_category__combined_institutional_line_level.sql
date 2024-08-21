{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combine_line_models as (


select claim_id
, claim_line_number
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_path_lab') }}

union all

select claim_id
, claim_line_number
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_pharmacy_institutional') }}

union all


select claim_id
, claim_line_number
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_substance_use_institutional') }}

union all

select claim_id
, claim_line_number
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_surgery_institutional') }}

union all

select claim_id
, claim_line_number
, service_category_2
, service_category_3
, tuva_last_run
, source_model_name
from {{ ref('service_category__outpatient_radiology_institutional') }}

)

select
  l.claim_id
, l.claim_line_number
, l.service_category_2
, l.service_category_3
, source_model_name
from combine_line_models l
 