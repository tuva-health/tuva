{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    patient_id
    , birth_date
    , gender
    , race
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__eligibility') }}