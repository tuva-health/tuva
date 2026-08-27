{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

select distinct
    ov.patient_data_source_id
    , ov.data_source
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , ov.old_encounter_id
from {{ ref('office_visits__int_office_visits') }} as ov
inner join {{ ref('encounters__stg_medical_claim') }} as mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
    and mc.data_source = ov.data_source
where substring(hcpcs_code, 1, 1) = 'J'
