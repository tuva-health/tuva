{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))
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
    , ingest_datetime
    , tuva_last_run
    , data_source
{%- endset -%}

{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true and the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized__location')) }}
{%- endset -%}

with loc as (
    {{ smart_union([ref('core__stg_claims_location'), ref('normalized__location')], source_index='_record_source') }}
),

prioritized_locations as (
    select
          loc.*
        , max(case when loc._record_source = 2 then 1 else 0 end) over (
              partition by loc.location_id, loc.data_source
          ) as _has_clinical_record
    from loc
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from prioritized_locations
where _record_source = 2
   or location_id is null
   or data_source is null
   or _has_clinical_record = 0

{% elif the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized__location')) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('normalized__location') }}

{% elif the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
{# No extension columns — input_layer__location is clinical-only #}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_location') }}

{%- endif %}
