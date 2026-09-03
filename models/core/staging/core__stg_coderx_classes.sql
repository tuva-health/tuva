{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     materialized = 'ephemeral'
   )
}}

{%- set coderx_name_type = 'string' if target.type in ['bigquery', 'databricks'] else 'varchar(3000)' -%}
{%- set coderx_string_type = 'varchar(256)' if target.type == 'redshift' else dbt.type_string() -%}

select
      drug_id
    , drug_name
    , atc_1_code
    , atc_1_name
    , atc_2_code
    , atc_2_name
    , atc_3_code
    , atc_3_name
    , atc_4_code
    , atc_4_name
from (
    select
          *
        , row_number() over (
              partition by drug_id
              order by
                    case when atc_3_code is null then 1 else 0 end
                  , coalesce(atc_3_code, '')
                  , coalesce(atc_3_name, '')
                  , coalesce(atc_4_code, '')
                  , coalesce(atc_4_name, '')
                  , coalesce(atc_2_code, '')
                  , coalesce(atc_2_name, '')
                  , coalesce(atc_1_code, '')
                  , coalesce(atc_1_name, '')
                  , coalesce(drug_name, '')
          ) as class_rank
    from (
        select
              nullif(cast(drug_id as {{ coderx_string_type }}), 'NULL') as drug_id
            , nullif(cast(drug_name as {{ coderx_name_type }}), 'NULL') as drug_name
            , nullif(cast(atc_1_code as {{ coderx_string_type }}), 'NULL') as atc_1_code
            , nullif(cast(atc_1_name as {{ coderx_name_type }}), 'NULL') as atc_1_name
            , nullif(cast(atc_2_code as {{ coderx_string_type }}), 'NULL') as atc_2_code
            , nullif(cast(atc_2_name as {{ coderx_name_type }}), 'NULL') as atc_2_name
            , nullif(cast(atc_3_code as {{ coderx_string_type }}), 'NULL') as atc_3_code
            , nullif(cast(atc_3_name as {{ coderx_name_type }}), 'NULL') as atc_3_name
            , nullif(cast(atc_4_code as {{ coderx_string_type }}), 'NULL') as atc_4_code
            , nullif(cast(atc_4_name as {{ coderx_name_type }}), 'NULL') as atc_4_name
        {% if the_tuva_project.tuva_boolean_var('use_coderx_enterprise', false) %}
        from {{ source('coderx', 'classes') }}
        {% else %}
        from {{ ref('terminology__coderx_classes') }}
        {% endif %}
    ) as source_rows
) as ranked
where class_rank = 1
