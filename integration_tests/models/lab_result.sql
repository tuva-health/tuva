{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      lab_result_id
    , person_id
    , patient_id
    , encounter_id
    , accession_number
    , source_order_type
    , source_order_code
    , source_order_description
    , source_component_type
    , source_component_code
    , source_component_description
    , normalized_order_type
    , normalized_order_code
    , normalized_order_description
    , normalized_component_type
    , normalized_component_code
    , normalized_component_description
    , status
    , result
    , result_datetime
    , collection_datetime
    , source_units
    , normalized_units
    , source_reference_range_low
    , source_reference_range_high
    , normalized_reference_range_low
    , normalized_reference_range_high
    , source_abnormal_flag
    , normalized_abnormal_flag
    , specimen
    , ordering_practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , lab_result_id as x_temp_lab_result_id #}
    {# , person_id as x_temp_person_id #}
    {# , source_component_type as x_temp_source_component_type #}
    {# , source_order_type as zzz_temp_source_order_type #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('lab_result_seed') }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'lab_result') }}

{%- endif %}
