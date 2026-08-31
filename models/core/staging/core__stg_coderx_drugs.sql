{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     materialized = 'ephemeral'
   )
}}

{%- set coderx_name_type = 'string' if target.type in ['bigquery', 'databricks'] else 'varchar(3000)' -%}
{%- set coderx_string_type = 'varchar(256)' if target.type == 'redshift' else dbt.type_string() -%}

select
      nullif(cast(drug_id as {{ coderx_string_type }}), 'NULL') as drug_id
    , nullif(cast(drug_name as {{ coderx_name_type }}), 'NULL') as drug_name
    , nullif(cast(is_brand as {{ coderx_string_type }}), 'NULL') as is_brand
    , nullif(cast(clinical_drug_id as {{ coderx_string_type }}), 'NULL') as clinical_drug_id
    , nullif(cast(clinical_drug_name as {{ coderx_name_type }}), 'NULL') as clinical_drug_name
    , nullif(cast(active as {{ coderx_string_type }}), 'NULL') as active
    , nullif(cast(prescribable as {{ coderx_string_type }}), 'NULL') as prescribable
{% if the_tuva_project.tuva_boolean_var('use_coderx_enterprise', false) %}
from {{ source('coderx', 'drugs') }}
{% else %}
from {{ ref('terminology__coderx_drugs') }}
{% endif %}
