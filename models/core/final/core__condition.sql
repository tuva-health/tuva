{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
   )
}}


{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_condition') }}
union all
select * from {{ ref('core__stg_clinical_condition') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_condition') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_condition') }}

{%- endif %}