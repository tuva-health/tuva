{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
apr_drg_code,
bill_type_code,
claim_id,
claim_line_number,
claim_type,
hcpcs_code,
ms_drg_code,
place_of_service_code,
revenue_center_code,
'{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__medical_claim') }}