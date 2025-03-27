{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
   med.claim_id
, med.claim_line_number
, 'outpatient' as service_category_1
, 'observation' as service_category_2
, 'observation' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
where claim_type = 'institutional'
and (revenue_center_code in ('0762')
or hcpcs_code in ('G0378', 'G0379')
)
