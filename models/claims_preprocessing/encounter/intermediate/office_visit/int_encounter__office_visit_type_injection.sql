{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select distinct
    ov.patient_data_source_id
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , ov.old_encounter_id
from {{ ref('int_encounter__office_visit_candidate') }} as ov
inner join {{ ref('int_encounter__claim_line') }} as mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
where substring(hcpcs_code, 1, 1) = 'J'
