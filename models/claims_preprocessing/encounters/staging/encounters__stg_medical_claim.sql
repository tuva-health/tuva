{{ config(
  enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select
    m.apr_drg_code
  , m.claim_id
  , m.claim_line_number
  , m.claim_id || cast(m.claim_line_number as {{dbt.type_string() }} ) as claim_line_id
  , m.claim_type
  , coalesce(m.admission_date,m.claim_start_date,m.claim_line_start_date) as start_date
  , coalesce(m.discharge_date,m.claim_end_date,m.claim_line_end_date) as end_date
  , g.service_category_1
  , g.service_category_2
  , g.service_category_3
  , m.bill_type_code
  , bt.bill_type_description
  , m.hcpcs_code
  , c.ccs_category
  , c.ccs_category_description
  , m.ms_drg_code
  , drg.ms_drg_description
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
  , n.modality
  , m.billing_id
  , '{{ var('tuva_last_run') }}' as tuva_last_run
  , row_number() over (order by random()) as rn
from {{ ref('normalized_input__medical_claim') }} m
inner join {{ ref('service_category__service_category_grouper') }} g on m.claim_id = g.claim_id
and
m.claim_line_number = g.claim_line_number
and
g.duplicate_row_number = 1
left join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} dx on m.diagnosis_code_1 = dx.icd_10_cm_code
left join {{ ref('terminology__provider') }} p on m.facility_id = p.npi
left join {{ ref('terminology__ccs_services_procedures') }} c on m.hcpcs_code = c.hcpcs_code
left join {{ ref('terminology__nitos') }} n on m.hcpcs_code = n.hcpcs_code
left join {{ ref('terminology__ms_drg') }} drg on m.ms_drg_code = drg.ms_drg_code
left join {{ ref('terminology__revenue_center') }} r on m.revenue_center_code = r.revenue_center_code
left join {{ ref('terminology__place_of_service') }} pos on m.place_of_service_code = pos.place_of_service_code
left join {{ ref('terminology__bill_type') }} bt on m.bill_type_code = bt.bill_type_code

