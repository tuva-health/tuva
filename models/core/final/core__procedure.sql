{{ config(
     enabled = var('tuva_marts_enabled',False)
   )
}}


{% if var('claims_enabled') == true and var('medical_records_enabled') == false -%}

select * from {{ ref('core__stg_claims_procedure') }}

{% elif var('medical_records_enabled') == true and var('claims_enabled') == false -%}

select * from {{ ref('core__stg_medical_records_procedure') }}

{% else %}

select * from {{ ref('core__stg_claims_procedure') }}
union all
select * from {{ ref('core__stg_medical_records_procedure') }}

{%- endif %}


