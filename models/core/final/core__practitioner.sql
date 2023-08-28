{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
   )
}}


{% if var('claims_enabled') == true and var('clinical_enabled') == false -%}

select * from {{ ref('core__stg_claims_practitioner') }}

{% elif var('clinical_enabled') == true and var('claims_enabled') == false -%}

select * from {{ ref('core__stg_medical_records_practitioner') }}

{% else %}

select * from {{ ref('core__stg_claims_practitioner') }}
union all
select * from {{ ref('core__stg_medical_records_practitioner') }}

{%- endif %}


