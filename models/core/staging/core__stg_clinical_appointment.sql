{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

select
    cast(appointment_id as {{ dbt.type_string() }}) as appointment_id
  , cast(patient_id as {{ dbt.type_string() }}) as patient_id
  , cast(provider_id as {{ dbt.type_string() }}) as provider_id
  , cast(status as {{ dbt.type_string() }}) as appt_status
  , cast(appointment_type_code as {{ dbt.type_string() }}) as appointment_type_code
  , cast('clinical' as {{ dbt.type_string() }}) as appointment_group
  , cast(start_time as {{ dbt.type_timestamp() }} ) as appointment_start_time
  , cast(end_time as {{ dbt.type_timestamp() }} ) as appointment_end_time
  , cast(reason_code as {{ dbt.type_string() }}) as reason_code
  , cast(reason_description as {{ dbt.type_string() }}) as reason_description
  , cast(cancellation_reason as {{ dbt.type_string() }}) as cancellation_reason
  , cast(location_id as {{ dbt.type_string() }}) as location_id
  , cast(null as {{ dbt.type_string() }}) as virtual_service
  , cast(null as {{ dbt.type_int() }}) as telehealth_flag
  , cast(null as {{ dbt.type_int() }}) as emergency_flag
  , cast(null as {{ dbt.type_int() }}) as home_visit_flag
  , cast(null as {{ dbt.type_int() }}) as specialist_referral_flag
  , cast(null as {{ dbt.type_int() }}) as primary_care_flag
  , cast(null as {{ dbt.type_int() }}) as preventive_care_flag
  , cast(null as {{ dbt.type_string() }}) as recurrence_id
  , cast(null as {{ dbt.type_string() }}) as recurrence_type
  , cast(null as {{ dbt.type_int() }}) as recurring_appointment_flag
  , cast(created_at as {{ dbt.type_timestamp() }}) as created_at
  , cast(updated_at as {{ dbt.type_timestamp() }}) as updated_at
  , cast(data_source as {{ dbt.type_string() }}) as data_source
  , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from {{ ref('input_layer__appointment') }}
