{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    ov.patient_data_source_id
    , ov.start_date
    , ov.claim_id
    , ov.claim_line_number
    , ov.old_encounter_id
from {{ ref('office_visits__int_office_visits') }} as ov
where service_category_2 = 'telehealth visit'
