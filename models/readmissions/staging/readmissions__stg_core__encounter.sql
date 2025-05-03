{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
    encounter_id
    , person_id
    , encounter_start_date
    , encounter_end_date
    , discharge_disposition_code
    , facility_id
    , drg_code_type
    , drg_code
    , paid_amount
    , primary_diagnosis_code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__encounter') }}
where encounter_type = 'acute inpatient'
