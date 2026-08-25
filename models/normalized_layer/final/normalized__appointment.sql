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
    , cast(appts.type as {{ dbt.type_string() }}) as type
    , cast(appts.status as {{ dbt.type_string() }}) as status
    , cast(appts.reason as {{ dbt.type_string() }}) as reason
    , cast(appts.cancellation_reason as {{ dbt.type_string() }}) as cancellation_reason
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__appointment'), alias='appts', strip_prefix=false) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(appts.data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('input_layer__appointment') }} as appts
