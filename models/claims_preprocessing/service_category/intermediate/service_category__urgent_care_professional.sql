{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_source as (
select distinct
    claim_id
    , claim_line_number
    , claim_line_id
, 'urgent care' as service_category_2
, 'urgent care' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
  and place_of_service_code in ('20')

union all

select distinct
    claim_id
    , claim_line_number
    , claim_line_id
, 'urgent care' as service_category_2
, 'urgent care' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
and hcpcs_code in ('S9088', '99051', 'S9083')
)

select distinct
claim_id
, claim_line_number
, claim_line_id
, 'outpatient' as service_category_1
, service_category_2
, service_category_3
, source_model_name
, tuva_last_run
from multiple_source
