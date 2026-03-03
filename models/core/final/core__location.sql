{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
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
    , data_source
    , tuva_last_run
{%- endset -%}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__location')) }}
{%- endset -%}

with loc as (
    {{ smart_union([ref('core__stg_claims_location'), ref('core__stg_clinical_location')], source_index=none) }}
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from loc

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__location')) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_clinical_location') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
{# No extension columns — input_layer__location is clinical-only #}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_location') }}

{%- endif %}
