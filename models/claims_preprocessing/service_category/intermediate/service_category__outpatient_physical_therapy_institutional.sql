{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    med.claim_id
    , med.claim_line_number
    , 'outpatient' as service_category_1
    , 'outpatient pt/ot/st' as service_category_2
    , 'outpatient pt/ot/st' as service_category_3
    , '{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as o on med.claim_id = o.claim_id
where ccs_category in ('213', '212', '215')
