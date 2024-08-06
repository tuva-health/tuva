{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
    ov.encounter_id
    ,mc.*
from {{ ref('office_visits__int_office_visits') }} ov
inner join {{ ref('encounters__stg_medical_claim') }} mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
inner join {{ ref('rbcs__hcpcs_to_rbcs') }} rbcs on mc.hcpcs_code = rbcs.hcpcs_cd
where 1 = 1
    and rbcs.rbcs_cat_desc = 'Imaging'