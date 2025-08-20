{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('enable_normalize_engine',false) != true %}

select
      appts.appointment_id
    , appts.person_id
    , appts.patient_id
    , appts.encounter_id
    , appts.source_appointment_type_code
    , appts.source_appointment_type_description
    , case
        when appointment_type.code is not null then appointment_type.code
        when appts.normalized_appointment_type_code is not null then appts.normalized_appointment_type_code
        else null
      end as normalized_appointment_type_code
    , case
        when appointment_type.code is not null then appointment_type.description
        when appts.normalized_appointment_type_description is not null then appts.normalized_appointment_type_description
        else null
      end as normalized_appointment_type_description
    , appts.start_datetime
    , appts.end_datetime
    , appts.duration
    , appts.location_id
    , appts.practitioner_id
    , appts.source_status
    , case
        when appointment_status.code is not null then appointment_status.code
        when appts.normalized_status is not null then appts.normalized_status
        else null
      end as normalized_status
    , appts.appointment_specialty
    , appts.reason
    , appts.source_reason_code_type
    , appts.source_reason_code
    , appts.source_reason_description
    , case
        when icd10.icd_10_cm is not null then 'icd-10-cm'
        when icd9.icd_9_cm is not null then 'icd-9-cm'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        when appts.normalized_reason_code_type is not null then appts.normalized_reason_code_type
        else null
      end as normalized_reason_code_type
    , coalesce(
          icd10.icd_10_cm
        , icd9.icd_9_cm
        , snomed_ct.snomed_ct
        , appts.normalized_reason_code
      ) as normalized_reason_code
    , coalesce(
          icd10.short_description
        , icd9.short_description
        , snomed_ct.description
        , appts.normalized_reason_description
      ) as normalized_reason_description
    , appts.cancellation_reason
    , appts.source_cancellation_reason_code_type
    , appts.source_cancellation_reason_code
    , appts.source_cancellation_reason_description
    , case
        when appointment_cancellation_reason.code is not null then 'appointment-cancellation-reason'
        when appts.normalized_cancellation_reason_code_type is not null then appts.normalized_cancellation_reason_code_type
        else null
      end as normalized_cancellation_reason_code_type
    , coalesce(
          appointment_cancellation_reason.code
        , appts.normalized_cancellation_reason_code
      ) as normalized_cancellation_reason_code
    , coalesce(
          appointment_cancellation_reason.description
        , appts.normalized_cancellation_reason_description
      ) as normalized_cancellation_reason_description
    , appts.data_source
    , appts.tuva_last_run
from {{ ref('core__stg_clinical_appointment') }} as appts
    left outer join {{ ref('terminology__appointment_cancellation_reason') }} as appointment_cancellation_reason
        on appts.source_cancellation_reason_code = appointment_cancellation_reason.code
    left outer join {{ ref('terminology__appointment_status') }} as appointment_status
        on appts.source_status = appointment_status.code
    left outer join {{ ref('terminology__appointment_type') }} as appointment_type
        on appts.source_appointment_type_code = appointment_type.code
    left outer join {{ ref('terminology__icd_10_cm') }} as icd10
        on replace(appts.source_reason_code, '.', '') = icd10.icd_10_cm
    left outer join {{ ref('terminology__icd_9_cm') }} as icd9
        on replace(appts.source_reason_code, '.', '') = icd9.icd_9_cm
    left outer join {{ ref('terminology__snomed_ct') }} as snomed_ct
        on appts.source_reason_code = snomed_ct.snomed_ct

 {% else %}

select
      appts.appointment_id
    , appts.person_id
    , appts.patient_id
    , appts.encounter_id
    , appts.source_appointment_type_code
    , appts.source_appointment_type_description
    , case
        when appointment_type.code is not null then appointment_type.code
        when appts.normalized_appointment_type_code is not null then appts.normalized_appointment_type_code
        else custom_mapped_appointment_type.normalized_code
      end as normalized_appointment_type_code
    , case
        when appointment_type.code is not null then appointment_type.description
        when appts.normalized_appointment_type_description is not null then appts.normalized_appointment_type_description
        else custom_mapped_appointment_type.normalized_description
      end as normalized_appointment_type_description
    , appts.start_datetime
    , appts.end_datetime
    , appts.duration
    , appts.location_id
    , appts.practitioner_id
    , appts.source_status
    , case
        when appointment_status.code is not null then appointment_status.code
        when appts.normalized_status is not null then appts.normalized_status
        else custom_mapped_appointment_status.normalized_code
      end as normalized_status
    , appts.appointment_specialty
    , appts.reason
    , appts.source_reason_code_type
    , appts.source_reason_code
    , appts.source_reason_description
    , case
        when icd10.icd_10_cm is not null then 'icd-10-cm'
        when icd9.icd_9_cm is not null then 'icd-9-cm'
        when snomed_ct.snomed_ct is not null then 'snomed-ct'
        when appts.normalized_reason_code_type is not null then appts.normalized_reason_code_type
        else custom_mapped_reason_code.normalized_code_type
      end as normalized_reason_code_type
    , coalesce(
          icd10.icd_10_cm
        , icd9.icd_9_cm
        , snomed_ct.snomed_ct
        , appts.normalized_reason_code
        , custom_mapped_reason_code.normalized_code
      ) as normalized_reason_code
    , coalesce(
          icd10.short_description
        , icd9.short_description
        , snomed_ct.description
        , appts.normalized_reason_description
        , custom_mapped_reason_code.normalized_description
      ) as normalized_reason_description
    , appts.cancellation_reason
    , appts.source_cancellation_reason_code_type
    , appts.source_cancellation_reason_code
    , appts.source_cancellation_reason_description
    , case
        when appointment_cancellation_reason.code is not null then 'appointment-cancellation-reason'
        when appts.normalized_cancellation_reason_code_type is not null then appts.normalized_cancellation_reason_code_type
        else custom_mapped_cancellation_reason_code.normalized_code_type
      end as normalized_cancellation_reason_code_type
    , coalesce(
          appointment_cancellation_reason.code
        , appts.normalized_cancellation_reason_code
        , custom_mapped_cancellation_reason_code.normalized_code
      ) as normalized_cancellation_reason_code
    , coalesce(
          appointment_cancellation_reason.description
        , appts.normalized_cancellation_reason_description
        , custom_mapped_cancellation_reason_code.normalized_description
      ) as normalized_cancellation_reason_description
    , appts.data_source
    , appts.tuva_last_run
from {{ ref('core__stg_clinical_appointment') }} as appts
    left outer join {{ ref('terminology__appointment_cancellation_reason') }} as appointment_cancellation_reason
        on appts.source_cancellation_reason_code = appointment_cancellation_reason.code
    left outer join {{ ref('terminology__appointment_status') }} as appointment_status
        on appts.source_status = appointment_status.code
    left outer join {{ ref('terminology__appointment_type') }} as appointment_type
        on appts.source_appointment_type_code = appointment_type.code
    left outer join {{ ref('terminology__icd_10_cm') }} as icd10
        on replace(appts.source_reason_code, '.', '') = icd10.icd_10_cm
    left outer join {{ ref('terminology__icd_9_cm') }} as icd9
        on replace(appts.source_reason_code, '.', '') = icd9.icd_9_cm
    left outer join {{ ref('terminology__snomed_ct') }} as snomed_ct
        on appts.source_reason_code = snomed_ct.snomed_ct
    left outer join {{ ref('custom_mapped') }} as custom_mapped_appointment_status
        on lower(custom_mapped_appointment_status.source_code_type) = 'appointment_status'
        and custom_mapped_appointment_status.source_code = appts.source_status
    left outer join {{ ref('custom_mapped') }} as custom_mapped_appointment_type
        on lower(custom_mapped_appointment_type.source_code_type) = 'appointment_type'
        and custom_mapped_appointment_type.source_code = appts.source_appointment_type_code
    left outer join {{ ref('custom_mapped') }} as custom_mapped_cancellation_reason_code
        on lower(custom_mapped_cancellation_reason_code.source_code_type) = lower(appts.source_cancellation_reason_code_type)
        and custom_mapped_cancellation_reason_code.source_code = appts.source_cancellation_reason_code
    left outer join {{ ref('custom_mapped') }} as custom_mapped_reason_code
        on lower(custom_mapped_reason_code.source_code_type) = lower(appts.source_reason_code_type)
        and custom_mapped_reason_code.source_code = appts.source_reason_code

{% endif %}
