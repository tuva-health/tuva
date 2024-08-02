{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select 
    ov.encounter_id
    ,mc.*
from {{ ref('encounters__int_office_visits')}} ov
inner join {{ ref('encounters__stg_medical_claim')}} mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
left join {{ ref('terminology__provider')}} provider on mc.rendering_id = provider.npi
where 
    primary_specialty_description IN (
        'Occupational Health'
        ,'Occupational Medicine'
        ,'Occupational Therapist in Private Practice'
        ,'Occupational Therapy Assistant'
        ,'Physical Therapist'
        ,'Physical Therapist in Private Practice'
        ,'Physical Therapy Assistant'
        ,'Speech Language Pathologist'
        ,'Speech-Language Assistant'
    )