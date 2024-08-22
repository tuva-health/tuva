{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    ov.patient_id
    ,ov.start_date
    ,ov.claim_id
    ,ov.claim_line_number
    ,ov.old_encounter_id
from {{ ref('office_visits__int_office_visits')}} ov
inner join {{ ref('encounters__stg_medical_claim')}} mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
where 
    mc.rend_primary_specialty_description IN (
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