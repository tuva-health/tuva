{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

{%- set coderx_name_type = 'string' if target.type in ['bigquery', 'databricks'] else 'varchar(3000)' -%}

select
      cast(pharmacy_claim_id as {{ dbt.type_string() }}) as medication_id
    , cast('claims' as {{ dbt.type_string() }}) as source_type
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as encounter_id
    , cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(claim_line_number as {{ dbt.type_int() }}) as claim_line_number
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , dispensing_date
    , cast(null as date) as prescribing_date
    , cast('ndc' as {{ dbt.type_string() }}) as source_code_type
    , cast(ndc_code as {{ dbt.type_string() }}) as source_code
    , cast(ndc_description as {{ coderx_name_type }}) as source_description
    , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(ndc_description as {{ coderx_name_type }}) as ndc_description
    , cast(null as {{ dbt.type_string() }}) as rxnorm_code
    , cast(null as {{ dbt.type_string() }}) as atc_code
    , cast(null as {{ dbt.type_string() }}) as route
    , cast(null as {{ dbt.type_string() }}) as strength
    , cast(quantity as {{ dbt.type_int() }}) as quantity
    , cast(null as {{ dbt.type_string() }}) as quantity_unit
    , cast(days_supply as {{ dbt.type_int() }}) as days_supply
    , cast(prescribing_provider_id as {{ dbt.type_string() }}) as practitioner_id
    , ingest_datetime
    , tuva_last_run
    , data_source
from {{ ref('core__pharmacy_claim') }}
