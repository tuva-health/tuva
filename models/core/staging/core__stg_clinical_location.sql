{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(location_id as {{ dbt.type_string() }}) as location_id
    , cast(npi as {{ dbt.type_string() }}) as npi
    , cast(name as {{ dbt.type_string() }}) as name
    , cast(facility_type as {{ dbt.type_string() }}) as facility_type
    , cast(parent_organization as {{ dbt.type_string() }}) as parent_organization
    , cast(address as {{ dbt.type_string() }}) as address
    , cast(city as {{ dbt.type_string() }}) as city
    , cast(state as {{ dbt.type_string() }}) as state
    , cast(zip_code as {{ dbt.type_string() }}) as zip_code
    , cast(latitude as {{ dbt.type_float() }}) as latitude
    , cast(longitude as {{ dbt.type_float() }}) as longitude
{%- endset -%}

{%- set tuva_metadata_columns -%}
      , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__location'), strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__location') }}
