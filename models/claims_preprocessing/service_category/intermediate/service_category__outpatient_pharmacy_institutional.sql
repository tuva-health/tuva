{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    , med.claim_line_number
    , 'Emergency Department' as service_category_2
    , 'Emergency Department' as service_category_3
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
where claim_type = 'institutional'
and (substring(revenue_center_code,1,3) in ('025' --pharmacy
,'026' --iv therapy
,'063' --pharmacy
,'089' --pharmacy
)
or revenue_center_code = '0547'
)