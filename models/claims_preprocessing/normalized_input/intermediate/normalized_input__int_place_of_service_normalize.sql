{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
    claim_id
    , claim_line_number
    , data_source
    , pos.place_of_service_code as normalized_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__place_of_service') }} pos
    on lpad(med.place_of_service_code, 2, '0') = pos.place_of_service_code
where claim_type = 'professional'