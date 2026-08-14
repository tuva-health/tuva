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
where service_category_2 = 'office-based surgery'
