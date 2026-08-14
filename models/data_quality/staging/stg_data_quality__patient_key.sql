{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool)
   )
}}

select distinct
      person_id
    , patient_id
    , data_source
from {{ ref('stg_input_layer__patient') }}
