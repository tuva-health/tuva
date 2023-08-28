{{ config(
     enabled = var('medical_records_enabled',var('tuva_marts_enabled',False))
   )
}}

select
    patient_id
    , first_name
    , last_name
    , sex
    , race
    , birth_date
    , death_date
    , death_flag
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , data_source
    , tuva_last_run
from {{ ref('core_stage_clinical__patient') }}