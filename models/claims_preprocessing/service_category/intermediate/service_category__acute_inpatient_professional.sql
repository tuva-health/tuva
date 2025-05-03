{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    claim_id
  , claim_line_number
  , claim_line_id
  , 'inpatient' as service_category_1
  , 'acute inpatient' as service_category_2
  , case
      when hcpcs_code in ('59400', '59409', '59410', '59610', '59612', '59614') then 'l/d - vaginal delivery'
      when hcpcs_code in ('59510', '59514', '59515', '59618', '59620', '59622') then 'l/d - cesarean delivery'
      else 'acute inpatient - other'
    end as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where
  claim_type = 'professional'
  and place_of_service_code = '21'
