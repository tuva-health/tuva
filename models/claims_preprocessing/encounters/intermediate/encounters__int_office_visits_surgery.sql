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
where
    hcpcs_code REGEXP '^[0-9]{5}$'
    AND ( 
        hcpcs_code BETWEEN '10021' AND '69999'
        OR
        hcpcs_code BETWEEN '90281' AND '99607'
    )