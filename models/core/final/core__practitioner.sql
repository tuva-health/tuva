{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true-%}

select * from {{ ref('core__stg_claims_practitioner') }}
union all
select * from {{ ref('core__stg_clinical_practitioner') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_practitioner') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_practitioner') }}

{%- endif %}


