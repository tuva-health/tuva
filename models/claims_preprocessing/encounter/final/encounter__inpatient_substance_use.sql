{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

{{ inpatient_encounter_final('inpatient_substance_use') }}
