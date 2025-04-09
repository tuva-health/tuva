{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{% if var('clinical_enabled', false) == true and var('claims_enabled', false) == true -%}

    select *, 'claim' as encounter_source_type
    from {{ ref('core__stg_claims_encounter') }}

    union all

    select *, 'clinical' as encounter_source_type
    from {{ ref('core__stg_clinical_encounter') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *, 'clinical' as encounter_source_type
    from {{ ref('core__stg_clinical_encounter') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *, 'claim' as encounter_source_type
    from {{ ref('core__stg_claims_encounter') }}

{%- endif %}
