{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct
    ov.patient_data_source_id
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , mc.hcpcs_code
    /* No longer offset past max(old_encounter_id) from office_visit_candidate.
       That offset kept these radiology ids from colliding with the base office
       visit ids, which were sequential integers. These keys include hcpcs_code
       while the candidate keys do not, so the two key spaces cannot collide.
       Dropping it also removes a max() over the candidate table and a cross join. */
    , {{ dbt_utils.generate_surrogate_key(['ov.patient_data_source_id', 'ov.start_date', 'mc.hcpcs_code']) }} as old_encounter_id
from {{ ref('int_encounter__office_visit_candidate') }} as ov
inner join {{ ref('int_encounter__claim_line') }} as mc on mc.claim_id = ov.claim_id
    and mc.claim_line_number = ov.claim_line_number
inner join {{ ref('int_service_category__office_based_radiology_professional') }} as scrad on mc.claim_id = scrad.claim_id
    and mc.claim_line_number = scrad.claim_line_number
