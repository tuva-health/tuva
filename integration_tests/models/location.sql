{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

{%- set tuva_columns -%}
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

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , state as x_temp_state #}
    {# , parent_organization as x_temp_parent_organization #}
    {# , facility_type as zzz_temp_facility_type #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
{% if the_tuva_project.tuva_synthetic_data_enabled() %}
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__location') }}
{% endif %}
from {{ source('source_input', 'location') }}
