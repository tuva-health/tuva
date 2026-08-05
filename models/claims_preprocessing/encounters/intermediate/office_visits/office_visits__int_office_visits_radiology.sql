{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select distinct
    ov.patient_data_source_id
    , ov.data_source
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , mc.hcpcs_code
    , {{ the_tuva_project.encounter_id_hash(["'office visit radiology'", 'ov.patient_data_source_id', 'ov.start_date', 'mc.hcpcs_code']) }} as old_encounter_id
from {{ ref('office_visits__int_office_visits') }} as ov
inner join {{ ref('encounters__stg_medical_claim') }} as mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
    and mc.data_source = ov.data_source
inner join {{ ref('service_category__office_based_radiology') }} as scrad on mc.claim_id = scrad.claim_id
    and mc.claim_line_number = scrad.claim_line_number
    and mc.data_source = scrad.data_source
