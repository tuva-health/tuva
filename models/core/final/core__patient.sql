
{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_patient') }}
union all
select * from {{ ref('core__stg_clinical_patient') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_clinical_patient') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select * from {{ ref('core__stg_claims_patient') }}

{%- endif %}
