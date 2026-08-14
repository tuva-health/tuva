{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with max_encounter as (
    select max(old_encounter_id) as max_encounter_id
    from {{ ref('int_encounter__office_visit_candidate') }}
)

select distinct
    ov.patient_data_source_id
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , mc.hcpcs_code
    , dense_rank() over (
order by ov.patient_data_source_id, ov.start_date, mc.hcpcs_code) + mx.max_encounter_id as old_encounter_id
from {{ ref('int_encounter__office_visit_candidate') }} as ov
cross join max_encounter as mx
inner join {{ ref('int_encounter__claim_line') }} as mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
inner join {{ ref('int_service_category__office_based_radiology_professional') }} as scrad on mc.claim_id = scrad.claim_id
    and mc.claim_line_number = scrad.claim_line_number
