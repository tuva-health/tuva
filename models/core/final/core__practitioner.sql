{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))
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
    , ingest_datetime
    , tuva_last_run
    , data_source
{%- endset -%}

{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true and the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized__practitioner')) }}
{%- endset -%}

with prac as (
    {{ smart_union([ref('core__stg_claims_practitioner'), ref('normalized__practitioner')], source_index='_record_source') }}
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from prac
where prac._record_source = 2
   or not exists (
        select 1
        from prac as clinical_practitioner
        where clinical_practitioner._record_source = 2
          and clinical_practitioner.practitioner_id = prac.practitioner_id
          and clinical_practitioner.data_source = prac.data_source
   )

{% elif the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized__practitioner')) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('normalized__practitioner') }}

{% elif the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

{%- set tuva_extension_columns -%}
{# No extension columns — input_layer__practitioner is clinical-only #}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_practitioner') }}

{%- endif %}
