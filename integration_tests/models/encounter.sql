{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

{%- set tuva_columns -%}
      encounter_id
    , person_id
    , patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , length_of_stay
    , admit_source_code
    , admit_source_description
    , admit_type_code
    , admit_type_description
    , discharge_disposition_code
    , discharge_disposition_description
    , attending_provider_id
    , attending_provider_name
    , facility_npi
    , facility_name
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , primary_diagnosis_description
    , drg_code_type
    , drg_code
    , drg_description
    , paid_amount
    , allowed_amount
    , charge_amount
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , encounter_type as x_temp_encounter_type #}
    {# , encounter_start_date as x_temp_encounter_start_date #}
    {# , facility_name as zzz_temp_facility_name #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ tuva_source('encounter') }}
