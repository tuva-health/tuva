{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
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
    , facility_id
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

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_encounter_type #}
    {# , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as x_temp_encounter_start_date #}
    {# , cast(null as {{ dbt.type_string() }}) as zzz_temp_facility_name #}
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
  cast(null as {{ dbt.type_string() }}) as encounter_id
, cast(null as {{ dbt.type_string() }}) as person_id
, cast(null as {{ dbt.type_string() }}) as patient_id
, cast(null as {{ dbt.type_string() }}) as encounter_type
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_start_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_end_date
, cast(null as {{ dbt.type_int() }}) as length_of_stay
, cast(null as {{ dbt.type_string() }}) as admit_source_code
, cast(null as {{ dbt.type_string() }}) as admit_source_description
, cast(null as {{ dbt.type_string() }}) as admit_type_code
, cast(null as {{ dbt.type_string() }}) as admit_type_description
, cast(null as {{ dbt.type_string() }}) as discharge_disposition_code
, cast(null as {{ dbt.type_string() }}) as discharge_disposition_description
, cast(null as {{ dbt.type_string() }}) as attending_provider_id
, cast(null as {{ dbt.type_string() }}) as attending_provider_name
, cast(null as {{ dbt.type_string() }}) as facility_id
, cast(null as {{ dbt.type_string() }}) as facility_name
, cast(null as {{ dbt.type_string() }}) as primary_diagnosis_code_type
, cast(null as {{ dbt.type_string() }}) as primary_diagnosis_code
, cast(null as {{ dbt.type_string() }}) as primary_diagnosis_description
, cast(null as {{ dbt.type_string() }}) as drg_code_type
, cast(null as {{ dbt.type_string() }}) as drg_code
, cast(null as {{ dbt.type_string() }}) as drg_description
, cast(null as {{ dbt.type_float() }}) as paid_amount
, cast(null as {{ dbt.type_float() }}) as allowed_amount
, cast(null as {{ dbt.type_float() }}) as charge_amount
{{ tuva_synthetic_extensions }}
, cast(null as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
{{ limit_zero() }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'encounter') }}

{%- endif %}
