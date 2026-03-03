{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(condition_id as {{ dbt.type_string() }}) as condition_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , {{ try_to_cast_date('recorded_date', 'YYYY-MM-DD') }} as recorded_date
    , {{ try_to_cast_date('onset_date', 'YYYY-MM-DD') }} as onset_date
    , {{ try_to_cast_date('resolved_date', 'YYYY-MM-DD') }} as resolved_date
    , cast(status as {{ dbt.type_string() }}) as status
    , cast(condition_type as {{ dbt.type_string() }}) as condition_type
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(normalized_code_type as {{ dbt.type_string() }}) as normalized_code_type
    , cast(normalized_code as {{ dbt.type_string() }}) as normalized_code
    , cast(normalized_description as {{ dbt.type_string() }}) as normalized_description
    , cast(condition_rank as {{ dbt.type_int() }}) as condition_rank
    , cast(present_on_admit_code as {{ dbt.type_string() }}) as present_on_admit_code
    , cast(present_on_admit_description as {{ dbt.type_string() }}) as present_on_admit_description
{%- endset -%}

{%- set tuva_metadata_columns -%}
      , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__condition'), strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__condition') }}
