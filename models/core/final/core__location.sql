{{ config(
     enabled = ((var('claims_enabled', False) | string | lower) == 'true')
            or ((var('clinical_enabled', False) | string | lower) == 'true')
   )
}}

{%- set tuva_core_columns -%}
      location_id
    , npi
    , name
    , facility_type
    , parent_organization
    , address
    , city
    , state
    , zip_code
    , latitude
    , longitude
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , tuva_last_run
    , data_source
{%- endset -%}

{% if (var('clinical_enabled', False) | string | lower) == 'true' and (var('claims_enabled', False) | string | lower) == 'true' -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized__location')) }}
{%- endset -%}

with loc as (
    {{ smart_union([ref('int_core__location_from_claim'), ref('normalized__location')], source_index=none) }}
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from loc

{% elif (var('clinical_enabled', False) | string | lower) == 'true' -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized__location')) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('normalized__location') }}

{% elif (var('claims_enabled', False) | string | lower) == 'true' -%}

{%- set tuva_extension_columns -%}
{# No extension columns — stg_input_layer__location is clinical-only #}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('int_core__location_from_claim') }}

{%- endif %}
