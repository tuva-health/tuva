{{ config(
     enabled = ((var('enable_data_quality', false) | string | lower) == 'true') and ((var('clinical_enabled', false) | string | lower) == 'true')
   )
}}

select distinct
      person_id
    , patient_id
    , data_source
from {{ ref('stg_input_layer__patient') }}
