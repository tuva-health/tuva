{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))
   )
}}

{%- set tuva_extension_columns_from_all_procedures -%}
{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) %}
    {{ select_extension_columns(ref('normalized__procedure'), alias='all_procedures') }}
{% endif %}
{%- endset -%}

{%- set tuva_metadata_columns_from_all_procedures -%}
    , all_procedures.ingest_datetime
    , all_procedures.tuva_last_run
    , all_procedures.data_source
{%- endset -%}

with all_procedures as (
{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true and the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

    {{ smart_union([ref('int_procedure_from_claims'), ref('normalized__procedure')]) }}

{% elif the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true -%}

    select *
    from {{ ref('normalized__procedure') }}

{% elif the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

    select *
    from {{ ref('int_procedure_from_claims') }}

{%- endif %}
)

select
    all_procedures.procedure_id
  , all_procedures.person_id
  , all_procedures.member_id
  , all_procedures.patient_id
  , all_procedures.encounter_id
  , all_procedures.claim_id
  , all_procedures.procedure_date
  , all_procedures.practitioner_id
  , all_procedures.code_system
  , all_procedures.source_code
  , all_procedures.source_description
  , all_procedures.normalized_code
  , all_procedures.normalized_description
  , procedure_grouper.procedure_family
  , procedure_grouper.{{ quote_column("procedure") }} as {{ quote_column("procedure") }}
  , all_procedures.modifier_1
  , all_procedures.modifier_2
  , all_procedures.modifier_3
  , all_procedures.modifier_4
  , all_procedures.modifier_5
  {{ tuva_extension_columns_from_all_procedures }}
  {{ tuva_metadata_columns_from_all_procedures }}
from all_procedures
left join {{ ref('tuva_procedure_grouper_code_map') }} as procedure_grouper
    on lower(all_procedures.code_system) = procedure_grouper.code_system
        and replace(all_procedures.normalized_code, '.', '') = procedure_grouper.code
