{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(npi as {{ dbt.type_string() }}) as npi
    , cast(first_name as {{ dbt.type_string() }}) as first_name
    , cast(last_name as {{ dbt.type_string() }}) as last_name
    , cast(practice_affiliation as {{ dbt.type_string() }}) as practice_affiliation
    , cast(specialty as {{ dbt.type_string() }}) as specialty
    , cast(sub_specialty as {{ dbt.type_string() }}) as sub_specialty
{%- endset -%}

{%- set tuva_metadata_columns -%}
      , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__practitioner'), strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__practitioner') }}
