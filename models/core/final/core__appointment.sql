{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(appts.appointment_id as {{ dbt.type_string() }}) as appointment_id
    , cast(appts.person_id as {{ dbt.type_string() }}) as person_id
    , cast(appts.patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(appts.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , {{ try_to_cast_datetime('appts.start_datetime') }} as start_datetime
    , {{ try_to_cast_datetime('appts.end_datetime') }} as end_datetime
    , cast(appts.duration as {{ dbt.type_int() }}) as duration
    , cast(appts.location_id as {{ dbt.type_string() }}) as location_id
    , cast(appts.practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(appts.type_code as {{ dbt.type_string() }}) as type_code
    , cast(appts.type_description as {{ dbt.type_string() }}) as type_description
    , appointment_type.code as type_code_norm
    , appointment_type.description as type_description_norm
    , cast(appts.status_code as {{ dbt.type_string() }}) as status_code
    , cast(appts.status_description as {{ dbt.type_string() }}) as status_description
    , appointment_status.code as status_code_norm
    , appointment_status.code as status_description_norm
    , cast(appts.reason as {{ dbt.type_string() }}) as reason
    , cast(appts.cancellation_reason as {{ dbt.type_string() }}) as cancellation_reason
    , appointment_cancellation_reason.code as cancellation_reason_code_norm
    , appointment_cancellation_reason.description as cancellation_reason_description_norm
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__appointment'), alias='appts', strip_prefix=false) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    , cast(appts.data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('input_layer__appointment') }} as appts
    left outer join {{ ref('terminology__appointment_type') }} as appointment_type
        on lower(trim(appts.type_code)) = lower(trim(appointment_type.code))
    left outer join {{ ref('terminology__appointment_status') }} as appointment_status
        on lower(trim(appts.status_code)) = lower(trim(appointment_status.code))
    left outer join {{ ref('terminology__appointment_cancellation_reason') }} as appointment_cancellation_reason
        on lower(trim(appts.cancellation_reason)) = lower(trim(appointment_cancellation_reason.description))
        or lower(trim(appts.cancellation_reason)) = lower(trim(appointment_cancellation_reason.code))
