{{ config(
  enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
  m.claim_id
  , m.claim_line_number
  , {{ concat_custom(["m.claim_id", "'|'", "cast(m.claim_line_number as " ~ dbt.type_string() ~ ")"]) }} as claim_line_id
  , m.claim_type
  , coalesce(m.admission_date, m.claim_line_start_date, m.claim_start_date) as start_date
  , coalesce(m.discharge_date, m.claim_line_end_date, m.claim_end_date) as end_date
  , m.admission_date
  , m.discharge_date
  , m.claim_start_date
  , m.claim_end_date
  , m.claim_line_start_date
  , m.claim_line_end_date
  , m.bill_type_code
  , bt.bill_type_description
  , m.hcpcs_code
  , c.ccs_category
  , c.ccs_category_description
  , m.drg_code_type
  , m.drg_code
  , m.drg_description
  , m.place_of_service_code
  , pos.place_of_service_description
  , m.revenue_center_code
  , r.revenue_center_description
  , m.diagnosis_code_1
  , dx.default_ccsr_category_ip
  , dx.default_ccsr_category_op
  , dx.default_ccsr_category_description_ip
  , dx.default_ccsr_category_description_op
  , p.primary_taxonomy_code
  , p.primary_specialty_description
  , rend.primary_specialty_description as rend_primary_specialty_description
  , n.modality
  , m.data_source
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__medical_claim') }} as m
left outer join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} as dx on m.diagnosis_code_1 = dx.icd_10_cm_code
left outer join {{ ref('terminology__provider') }} as p on m.facility_id = p.npi
left outer join {{ ref('terminology__ccs_services_procedures') }} as c on m.hcpcs_code = c.hcpcs_code
left outer join {{ ref('terminology__nitos') }} as n on m.hcpcs_code = n.hcpcs_code
left outer join {{ ref('terminology__revenue_center') }} as r on m.revenue_center_code = r.revenue_center_code
left outer join {{ ref('terminology__place_of_service') }} as pos on m.place_of_service_code = pos.place_of_service_code
left outer join {{ ref('terminology__bill_type') }} as bt on m.bill_type_code = bt.bill_type_code
left outer join {{ ref('terminology__provider') }} as rend on m.rendering_id = rend.npi
