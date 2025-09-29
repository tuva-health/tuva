{{ config(
     enabled = var('hedis_enabled', False) == True | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select * from {{ ref('hedis_measure_report_seed') }}

{%- else -%}

select * from {{ source('source_input', 'hedis_measure_report') }}

{%- endif %}
