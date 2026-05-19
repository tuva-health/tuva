{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(cond.condition_id as {{ dbt.type_string() }}) as condition_id
    , cast(null as {{ dbt.type_string() }}) as payer
    , cast(cond.person_id as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(cond.patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(cond.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as claim_id
    , {{ try_to_cast_date('cond.recorded_date', 'YYYY-MM-DD') }} as recorded_date
    , {{ try_to_cast_date('cond.onset_date', 'YYYY-MM-DD') }} as onset_date
    , {{ try_to_cast_date('cond.resolved_date', 'YYYY-MM-DD') }} as resolved_date
    , cast(cond.status as {{ dbt.type_string() }}) as status
    , cast(cond.condition_type as {{ dbt.type_string() }}) as condition_type
    , cast(cond.source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(cond.source_code as {{ dbt.type_string() }}) as source_code
    , cast(cond.source_description as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(cond.condition_rank as {{ dbt.type_int() }}) as condition_rank
    , cast(cond.present_on_admit_code as {{ dbt.type_string() }}) as present_on_admit_code
    , cast(poa.present_on_admit_description as {{ dbt.type_string() }}) as present_on_admit_description
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
      , cast(cond.data_source as {{ dbt.type_string() }}) as data_source
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__condition'), alias='cond', strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__condition') }} as cond
left outer join {{ ref('terminology__present_on_admission') }} as poa
    on cond.present_on_admit_code = poa.present_on_admit_code
