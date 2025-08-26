{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select * from {{ ref('immunization_seed') }}

{%- else -%}

select * from {{ source('source_input', 'immunization') }}

{%- endif %}
