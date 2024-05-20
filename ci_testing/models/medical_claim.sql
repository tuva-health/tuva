{% if var('test_data_override') == true -%}

select * from {{ ref('medical_claim_seed') }}

{%- else -%}

select * from {{ source('source_input', 'medical_claim') }}

{%- endif %}