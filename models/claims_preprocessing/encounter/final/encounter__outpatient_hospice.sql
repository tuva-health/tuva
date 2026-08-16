{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

{{ outpatient_encounter_final('outpatient_hospice') }}
