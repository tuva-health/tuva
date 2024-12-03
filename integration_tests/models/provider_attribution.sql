{{ config(
     enabled = (
         var('provider_attribution_enabled', False) == True and
         var('claims_enabled', var('tuva_marts_enabled', False))
     ) | as_bool
   )
}}


{% if var('use_synthetic_data') == true -%}

select * from {{ ref('provider_attribution_seed') }}

{%- else -%}

select * from {{ source('source_input', 'provider_attribution') }}

{%- endif %}



