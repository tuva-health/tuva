{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     materialized = 'ephemeral'
   )
}}

{%- set coderx_name_type = 'string' if target.type in ['bigquery', 'databricks'] else 'varchar(3000)' -%}

select
      nullif(cast(ndc11 as {{ dbt.type_string() }}), 'NULL') as ndc11
    , nullif(cast(ndc as {{ dbt.type_string() }}), 'NULL') as ndc
    , nullif(cast(drug_id as {{ dbt.type_string() }}), 'NULL') as drug_id
    , nullif(cast(drug_name as {{ coderx_name_type }}), 'NULL') as drug_name
    , nullif(cast(is_brand as {{ dbt.type_string() }}), 'NULL') as is_brand
    , nullif(cast(clinical_drug_id as {{ dbt.type_string() }}), 'NULL') as clinical_drug_id
    , nullif(cast(clinical_drug_name as {{ coderx_name_type }}), 'NULL') as clinical_drug_name
    , nullif(cast(active as {{ dbt.type_string() }}), 'NULL') as active
    , nullif(cast(prescribable as {{ dbt.type_string() }}), 'NULL') as prescribable
{% if the_tuva_project.tuva_boolean_var('use_coderx_enterprise', false) %}
from {{ source('coderx', 'packages') }}
{% else %}
from {{ ref('terminology__coderx_packages') }}
{% endif %}
