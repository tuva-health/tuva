{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
  med.claim_id
, med.claim_line_number
, med.claim_line_id
, 'outpatient' as service_category_1
, 'emergency department' as service_category_2
, 'emergency department' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_professional') }} as prof on med.claim_id = prof.claim_id
and
med.claim_line_number = prof.claim_line_number
where place_of_service_code = '23'
or
hcpcs_code in ('99281', '99282', '99283', '99284', '99285', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384')
