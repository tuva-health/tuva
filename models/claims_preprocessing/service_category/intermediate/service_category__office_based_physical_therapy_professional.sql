{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    med.claim_id
    , med.claim_line_number
    , med.claim_line_id
    , 'office-based' as service_category_1
    , 'office-based pt/ot/st' as service_category_2
    , 'office-based pt/ot/st' as service_category_3
    , '{{ this.name }}' as source_model_name
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_office_based') }} as prof on med.claim_id = prof.claim_id
and
med.claim_line_number = prof.claim_line_number
where (
ccs_category in ('213', '212', '215')
or
 med.rend_primary_specialty_description in (
        'Occupational Health'
        , 'Occupational Medicine'
        , 'Occupational Therapist in Private Practice'
        , 'Occupational Therapy Assistant'
        , 'Physical Therapist'
        , 'Physical Therapist in Private Practice'
        , 'Physical Therapy Assistant'
        , 'Speech Language Pathologist'
        , 'Speech-Language Assistant'
    ))
and place_of_service_code = '11'
