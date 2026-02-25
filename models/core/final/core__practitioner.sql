{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{%- set tuva_core_columns -%}
      practitioner_id
    , npi
    , first_name
    , last_name
    , practice_affiliation
    , specialty
    , sub_specialty
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , tuva_last_run
{%- endset -%}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__practitioner')) }}
{%- endset -%}

with prac as (
    {{ smart_union([ref('core__stg_claims_practitioner'), ref('core__stg_clinical_practitioner')], source_index=none) }}
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from prac

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__practitioner')) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_clinical_practitioner') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_extension_columns -%}
{# No extension columns — input_layer__practitioner is clinical-only #}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_practitioner') }}

{%- endif %}
