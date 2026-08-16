{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

{{ inpatient_encounter_generate_id('inpatient psychiatric') }}
