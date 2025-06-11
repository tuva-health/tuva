{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{% if var('clinical_enabled', false) == true and var('claims_enabled', false) == true -%}

  {{ the_tuva_project.custom_union_relations(
    relations=[
      ref('core__stg_claims_encounter'),
      ref('core__stg_clinical_encounter')
    ],
    exclude=["_loaded_at"]
  ) }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *
    from {{ ref('core__stg_clinical_encounter') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

    select *
    from {{ ref('core__stg_claims_encounter') }}

{%- endif %}
