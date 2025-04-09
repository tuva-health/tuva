{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_sources as (
select distinct person_id
, data_source
from {{ ref('normalized_input__medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct person_id
, data_source
from {{ ref('normalized_input__eligibility') }}
)

select
person_id
, data_source
, dense_rank() over (
order by concat(person_id, data_source)) as patient_data_source_id
from multiple_sources
