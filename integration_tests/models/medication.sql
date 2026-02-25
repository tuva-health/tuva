{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      medication_id
    , person_id
    , patient_id
    , encounter_id
    , dispensing_date
    , prescribing_date
    , source_code_type
    , source_code
    , source_description
    , ndc_code
    , ndc_description
    , rxnorm_code
    , rxnorm_description
    , atc_code
    , atc_description
    , route
    , strength
    , quantity
    , quantity_unit
    , days_supply
    , practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , rxnorm_code as x_temp_rxnorm_code #}
    {# , source_code_type as x_temp_source_code_type #}
    {# , source_code as zzz_temp_source_code #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_rxnorm_code #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_source_code_type #}
    {# , cast(null as {{ dbt.type_string() }}) as zzz_temp_source_code #}
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
  cast(null as {{ dbt.type_string() }}) as medication_id
, cast(null as {{ dbt.type_string() }}) as person_id
, cast(null as {{ dbt.type_string() }}) as patient_id
, cast(null as {{ dbt.type_string() }}) as encounter_id
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as prescribing_date
, cast(null as {{ dbt.type_string() }}) as source_code_type
, cast(null as {{ dbt.type_string() }}) as source_code
, cast(null as {{ dbt.type_string() }}) as source_description
, cast(null as {{ dbt.type_string() }}) as ndc_code
, cast(null as {{ dbt.type_string() }}) as ndc_description
, cast(null as {{ dbt.type_string() }}) as rxnorm_code
, cast(null as {{ dbt.type_string() }}) as rxnorm_description
, cast(null as {{ dbt.type_string() }}) as atc_code
, cast(null as {{ dbt.type_string() }}) as atc_description
, cast(null as {{ dbt.type_string() }}) as route
, cast(null as {{ dbt.type_string() }}) as strength
, cast(null as {{ dbt.type_int() }}) as quantity
, cast(null as {{ dbt.type_string() }}) as quantity_unit
, cast(null as {{ dbt.type_int() }}) as days_supply
, cast(null as {{ dbt.type_string() }}) as practitioner_id
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
from {{ source('source_input', 'medication') }}

{%- endif %}
