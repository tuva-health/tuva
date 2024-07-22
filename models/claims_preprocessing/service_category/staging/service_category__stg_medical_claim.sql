{{ config(
  enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    apr_drg_code
  , bill_type_code
  , claim_id
  , claim_line_number
  , claim_type
  , hcpcs_code
  , ms_drg_code
  , place_of_service_code
  , revenue_center_code
  , diagnosis_code_1
  , default_ccsr_category_ip
  , default_ccsr_category_op
  , default_ccsr_category_description_ip
  , default_ccsr_category_description_op
  , p.facility_primary_taxonomy_code
  , p.facility_primary_specialty_description
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__medical_claim') }} m
left join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} dx on m.diagnosis_code_1 = dx.icd_10_cm_code
left join {{ ref('terminology__provider') }} p on m.facility_id = p.npi
