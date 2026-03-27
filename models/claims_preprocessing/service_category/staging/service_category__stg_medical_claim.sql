{{ config(
  enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with ccs_release_year as (
  select
    max(release_year) as max_release_year
  from {{ ref('terminology__ccs_services_procedures') }}
)

, final as (
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
  from {{ ref('normalized_input__medical_claim') }} as m
  left outer join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} as dx on m.diagnosis_code_1 = dx.icd_10_cm_code
  left outer join {{ ref('terminology__provider') }} as p on m.facility_id = p.npi
  left outer join {{ ref('terminology__nitos') }} as n on m.hcpcs_code = n.hcpcs_code
  left outer join {{ ref('terminology__revenue_center') }} as r on m.revenue_center_code = r.revenue_center_code
  left outer join {{ ref('terminology__place_of_service') }} as pos on m.place_of_service_code = pos.place_of_service_code
  left outer join {{ ref('terminology__bill_type') }} as bt on m.bill_type_code = bt.bill_type_code
  left outer join {{ ref('terminology__provider') }} as rend on m.rendering_id = rend.npi
)

select
    f.claim_id
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
  , f.bill_type_code
  , f.bill_type_description
  , f.hcpcs_code
  , c.ccs_category
  , c.ccs_category_description
  , f.drg_code_type
  , f.drg_code
  , f.drg_description
  , f.place_of_service_code
  , f.place_of_service_description
  , f.revenue_center_code
  , f.revenue_center_description
  , f.diagnosis_code_1
  , f.default_ccsr_category_ip
  , f.default_ccsr_category_op
  , f.default_ccsr_category_description_ip
  , f.default_ccsr_category_description_op
  , f.primary_taxonomy_code
  , f.primary_specialty_description
  , f.rend_primary_specialty_description
  , f.modality
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
