{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with ccs_release_year as (
  select
    max(release_year) as max_release_year
  from {{ ref('terminology__ccs_services_procedures') }}
)

, final as (
  select
      m.person_id
    , d.patient_data_source_id
    , m.claim_id
    , m.claim_line_number
    , {{ concat_custom(["m.claim_id","'|'","cast(m.claim_line_number as " ~ dbt.type_string() ~ ")"]) }} as claim_line_id
    , m.claim_type
    , coalesce(m.admission_date, m.claim_line_start_date, m.claim_start_date) as start_date
    , coalesce(m.discharge_date, m.claim_line_end_date, m.claim_end_date) as end_date
    , m.admission_date
    , m.discharge_date
    , m.claim_start_date
    , m.claim_end_date
    , m.claim_line_start_date
    , m.claim_line_end_date
    , g.service_category_1
    , g.service_category_2
    , g.service_category_3
    , m.bill_type_code
    , bt.bill_type_description
    , m.hcpcs_code
    , m.hcpcs_modifier_1
    , m.hcpcs_modifier_2
    , m.hcpcs_modifier_3
    , m.hcpcs_modifier_4
    , m.hcpcs_modifier_5
    , m.drg_code_type
    , m.drg_code
    , coalesce(msdrg.ms_drg_description, aprdrg.apr_drg_description) as drg_description
    , m.admit_source_code
    , m.admit_type_code
    , m.place_of_service_code
    , pos.place_of_service_description
    , m.revenue_center_code
    , r.revenue_center_description
    , m.diagnosis_code_type
    , m.diagnosis_code_1
    , dx.default_ccsr_category_ip
    , dx.default_ccsr_category_op
    , dx.default_ccsr_category_description_ip
    , dx.default_ccsr_category_description_op
    , p.primary_taxonomy_code
    , p.primary_specialty_description
    , n.modality
    , m.billing_id
    , m.rendering_id
    , rend.primary_specialty_description as rend_primary_specialty_description
    , m.facility_id
    , m.discharge_disposition_code
    , m.paid_amount
    , m.charge_amount
    , m.allowed_amount
    , m.data_source
  from {{ ref('normalized_input__medical_claim') }} as m
  inner join {{ ref('service_category__service_category_grouper') }} as g on m.claim_id = g.claim_id
    and m.claim_line_number = g.claim_line_number
    and g.duplicate_row_number = 1
  inner join {{ ref('encounters__patient_data_source_id') }} as d on m.person_id = d.person_id
    and m.data_source = d.data_source
  left outer join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} as dx on m.diagnosis_code_1 = dx.icd_10_cm_code
  left outer join {{ ref('terminology__provider') }} as p on m.facility_id = p.npi
  left outer join {{ ref('terminology__nitos') }} as n on m.hcpcs_code = n.hcpcs_code
  left outer join {{ ref('terminology__ms_drg') }} as msdrg on m.drg_code_type = 'ms-drg' and m.drg_code = msdrg.ms_drg_code
  left outer join {{ ref('terminology__apr_drg') }} as aprdrg on m.drg_code_type = 'apr-drg' and m.drg_code = aprdrg.apr_drg_code
  left outer join {{ ref('terminology__revenue_center') }} as r on m.revenue_center_code = r.revenue_center_code
  left outer join {{ ref('terminology__place_of_service') }} as pos on m.place_of_service_code = pos.place_of_service_code
  left outer join {{ ref('terminology__bill_type') }} as bt on m.bill_type_code = bt.bill_type_code
  left outer join {{ ref('terminology__provider') }} as rend on m.rendering_id = rend.npi
)

select
    f.person_id
  , f.patient_data_source_id
  , f.claim_id
  , f.claim_line_number
  , f.claim_line_id
  , f.claim_type
  , f.start_date
  , f.end_date
  , f.admission_date
  , f.discharge_date
  , f.claim_start_date
  , f.claim_end_date
  , f.claim_line_start_date
  , f.claim_line_end_date
  , f.service_category_1
  , f.service_category_2
  , f.service_category_3
  , f.bill_type_code
  , f.bill_type_description
  , f.hcpcs_code
  , f.hcpcs_modifier_1
  , f.hcpcs_modifier_2
  , f.hcpcs_modifier_3
  , f.hcpcs_modifier_4
  , f.hcpcs_modifier_5
  , c.ccs_category
  , c.ccs_category_description
  , f.drg_code_type
  , f.drg_code
  , f.drg_description
  , f.admit_source_code
  , f.admit_type_code
  , f.place_of_service_code
  , f.place_of_service_description
  , f.revenue_center_code
  , f.revenue_center_description
  , f.diagnosis_code_type
  , f.diagnosis_code_1
  , f.default_ccsr_category_ip
  , f.default_ccsr_category_op
  , f.default_ccsr_category_description_ip
  , f.default_ccsr_category_description_op
  , f.primary_taxonomy_code
  , f.primary_specialty_description
  , f.modality
  , f.billing_id
  , f.rendering_id
  , f.rend_primary_specialty_description
  , f.facility_id
  , f.discharge_disposition_code
  , f.paid_amount
  , f.charge_amount
  , f.allowed_amount
  , f.data_source
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from final as f
cross join ccs_release_year as cry
left outer join {{ ref('terminology__ccs_services_procedures') }} as c
  on f.hcpcs_code = c.hcpcs_code
  and (
    (
      f.start_date >= c.start_valid_date
      and f.start_date <= c.end_valid_date
    )
    or (
      {{ date_part('year', 'f.start_date') }} > cry.max_release_year
      and c.release_year = cry.max_release_year
    )
  )
