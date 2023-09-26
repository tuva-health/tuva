{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    claim_id
    , patient_id
    , encounter_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , normalized_description
    , condition_rank
from {{ ref('core__condition') }}