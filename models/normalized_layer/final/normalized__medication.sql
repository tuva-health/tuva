{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}

{%- set tuva_core_columns -%}
      cast(medication_id as {{ dbt.type_string() }}) as medication_id
    , cast('clinical' as {{ dbt.type_string() }}) as source_type
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as claim_id
    , cast(null as {{ dbt.type_int() }}) as claim_line_number
    , cast(null as {{ dbt.type_string() }}) as payer
    , cast(null as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , {{ try_to_cast_date('dispensing_date', 'YYYY-MM-DD') }} as dispensing_date
    , {{ try_to_cast_date('prescribing_date', 'YYYY-MM-DD') }} as prescribing_date
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(null as {{ dbt.type_string() }}) as ndc_description
    , cast(rxnorm_code as {{ dbt.type_string() }}) as rxnorm_code
    , cast(atc_code as {{ dbt.type_string() }}) as atc_code
    , cast(route as {{ dbt.type_string() }}) as route
    , cast(strength as {{ dbt.type_string() }}) as strength
    , cast(quantity as {{ dbt.type_int() }}) as quantity
    , cast(quantity_unit as {{ dbt.type_string() }}) as quantity_unit
    , cast(days_supply as {{ dbt.type_int() }}) as days_supply
    , cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(data_source as {{ dbt.type_string() }}) as data_source
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__medication'), strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__medication') }}
