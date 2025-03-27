{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with max_encounter as (
    select max(old_encounter_id) as max_encounter_id
    from {{ ref('office_visits__int_office_visits') }}
)

select distinct
    ov.patient_data_source_id
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , mc.hcpcs_code
    , dense_rank() over (
order by ov.patient_data_source_id, ov.start_date, mc.hcpcs_code) + mx.max_encounter_id as old_encounter_id
from {{ ref('office_visits__int_office_visits') }} as ov
cross join max_encounter as mx
inner join {{ ref('encounters__stg_medical_claim') }} as mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
inner join {{ ref('service_category__office_based_radiology') }} as scrad on mc.claim_id = scrad.claim_id
    and mc.claim_line_number = scrad.claim_line_number
