{{ config(
     enabled = (var('enable_legacy_data_quality', false) | as_bool)
     and (var('clinical_enabled', False) | as_bool)
   )
}}

select * from {{ ref('immunization') }}
