{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

select
      immune.immunization_id
    , immune.person_id
    , immune.patient_id
    , immune.encounter_id
    , immune.source_code_type
    , immune.source_code
    , immune.source_description
    , immune.normalized_code_type
    , immune.normalized_code
    , immune.normalized_description
    , immune.status
    , immune.status_reason
    , immune.occurrence_date
    , immune.source_dose
    , immune.normalized_dose
    , immune.lot_number
    , immune.body_site
    , immune.route
    , immune.location_id
    , immune.practitioner_id
    {{ select_extension_columns(ref('normalized__immunization'), alias='immune', strip_prefix=false) }}
    , immune.tuva_last_run
    , immune.data_source
from {{ ref('normalized__immunization') }} as immune
