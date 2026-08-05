{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with multiple_sources as (
select distinct person_id
, data_source
from {{ ref('normalized__medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct person_id
, data_source
from {{ ref('normalized__eligibility') }}
)

select
person_id
, data_source
, {{ the_tuva_project.concat_custom(['person_id', "'|'", 'data_source']) }} as patient_data_source_id
from multiple_sources
