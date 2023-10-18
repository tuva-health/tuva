{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    patient_id
    , sex
    , birth_date
    , race
    , state
    , zip_code
    , latitude
    , longitude
from {{ ref('core__patient') }}