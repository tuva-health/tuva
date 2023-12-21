{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
    claim_id
    , claim_line_number
    , data_source
    , rev.revenue_center_code as normalized_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__revenue_center') }} rev
    on lpad(med.revenue_center_code, 4, '0') = rev.revenue_center_code
where claim_type = 'institutional'