{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
   )
}}


{% if var('claims_enabled') == true -%}

select * from {{ ref('core__stg_claims_practitioner') }}

{% elif var('clinical_enabled') == true -%}

select * from {{ ref('core__stg_clinical_practitioner') }}

{% else %}

select * from {{ ref('core__stg_claims_practitioner') }}
union all
select * from {{ ref('core__stg_clinical_practitioner') }}

{%- endif %}


