{% if var('test_data_override') == true -%}

select * from {{ ref('pharmacy_claim_seed') }}

{%- else -%}

select * from {{ source('source_input', 'pharmacy_claim') }}

{%- endif %}