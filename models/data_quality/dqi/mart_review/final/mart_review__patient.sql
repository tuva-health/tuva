{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

select *
    , {{ concat_custom([
        'person_id',
        "'|'",
        'data_source']) }} as patient_data_source_key
from {{ ref('core__patient') }}
