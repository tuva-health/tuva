{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
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

{%- set tuva_extensions -%}
    , 'lab_result' as x_tuva_test_extension
    , 'lab_result' as ext_tuva_test_extension
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('the_tuva_project', 'synthetic_data__lab_result') }}
