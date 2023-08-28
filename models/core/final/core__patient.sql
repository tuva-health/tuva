{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
   )
}}


{% if var('claims_enabled', false) == true -%}

select * from {{ ref('core__stg_claims_patient') }}

{% elif var('clinical_enabled', false) == true -%}

select * from {{ ref('core__stg_clinical_patient') }}

{% elif var('clinical_enabled', false) == true and var('claims_enabled', false) == true -%}

select * from {{ ref('core__stg_claims_patient') }}
union all
select * from {{ ref('core__stg_clinical_patient') }}

{%- endif %}


